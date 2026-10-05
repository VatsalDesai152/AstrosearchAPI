"""Complete production FastAPI REST API for AstroSearch."""



from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Literal

import httpx
import structlog
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from astronomy import ArchiveError, object_summary, summarize_system
from core import (
    AdvancedQuery,
    CacheManager,
    CatalogRegistry,
    CrossmatchService,
    ObjectResolutionError,
    SesameResolver,
    Settings,
    provider_map,
)
from datasets import DatasetEngine, MetadataStore, enqueue_dataset, process_dataset_async, submit_to_redis
from datasets import DatasetRequest as NormalizedDatasetRequest
from signals import RepresentationError, cross_reference_observation, plot_light_curves
from tess import TessLightCurveService, TessServiceError, TessServiceUnavailable

# ---------------------------------------------------------------------------
# Logging and Prometheus Metrics
# ---------------------------------------------------------------------------

logger = structlog.get_logger()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

REQUEST_COUNT = Counter("astrosearch_requests_total", "Total API requests", ["method", "endpoint", "status_code"])
REQUEST_LATENCY = Histogram("astrosearch_request_latency_seconds", "API request latency", ["method", "endpoint"])
ACTIVE_REQUESTS = Gauge("astrosearch_active_requests", "Active in-flight API requests", ["endpoint"])
CATALOG_QUERIES = Counter("astrosearch_catalog_queries_total", "Catalog query outcomes", ["catalog", "status"])
CATALOG_LATENCY = Histogram("astrosearch_catalog_query_seconds", "Catalog query latency", ["catalog"])


def setup_metrics(application: FastAPI) -> None:
    application.state.metrics = {
        "request_count": REQUEST_COUNT,
        "request_latency": REQUEST_LATENCY,
        "active_requests": ACTIVE_REQUESTS,
    }


# ---------------------------------------------------------------------------
# Authentication & Request Quotas
# ---------------------------------------------------------------------------


_QUOTA_WINDOW_SECONDS = 60
_REDIS_QUOTA_SCRIPT = """
local now = tonumber(ARGV[1])
local cutoff = now - tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', cutoff)
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
    return 0
end
redis.call('ZADD', KEYS[1], now, ARGV[4])
redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]) * 2000)
return 1
"""


class RequestQuota:
    """Sliding-window request quota tracker with Redis backend and in-memory fallback."""

    def __init__(self, redis_url: str | None = None, *, clock: Callable[[], float] = time.time) -> None:
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._clock = clock
        self._redis = None
        if redis_url:
            try:
                import redis.asyncio as aioredis
                self._redis = aioredis.from_url(redis_url)
            except Exception:
                self._redis = None

    async def allow(self, identity: str, limit: int) -> bool:
        if limit <= 0:
            return True
        now = self._clock()
        if self._redis is not None:
            key = f"astrosearch:quota:{identity}"
            try:
                allowed = await self._redis.eval(
                    _REDIS_QUOTA_SCRIPT,
                    1,
                    key,
                    now,
                    _QUOTA_WINDOW_SECONDS,
                    limit,
                    f"{now:.9f}:{uuid.uuid4().hex}",
                )
                return bool(allowed)
            except Exception as exc:
                logging.getLogger(__name__).warning("Redis quota unavailable: %s", exc)

        async with self._lock:
            events = self._events[identity]
            while events and events[0] <= now - _QUOTA_WINDOW_SECONDS:
                events.popleft()
            if len(events) >= limit:
                return False
            events.append(now)
            return True

    async def close(self) -> None:
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass


def authenticate(request: Request) -> str | JSONResponse:
    """Validate API key or JWT bearer token."""
    configured = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
    jwt_public_key = os.getenv("JWT_PUBLIC_KEY")
    jwt_secret = os.getenv("JWT_SECRET")
    required = os.getenv("REQUIRE_API_KEY", "false").lower() == "true"

    if required and not (configured or jwt_public_key or jwt_secret):
        return JSONResponse({"detail": "API authentication is not configured"}, status_code=503)

    authorization = request.headers.get("authorization", "")
    supplied = request.headers.get("x-api-key") or authorization.removeprefix("Bearer ")

    if configured and any(hmac.compare_digest(supplied, k) for k in configured):
        return hashlib.sha256(supplied.encode()).hexdigest()[:20]

    if authorization.startswith("Bearer ") and (jwt_public_key or jwt_secret):
        import jwt

        signing_key = jwt_public_key or jwt_secret
        assert signing_key is not None
        try:
            claims = jwt.decode(
                authorization.removeprefix("Bearer "),
                signing_key,
                algorithms=["RS256" if jwt_public_key else "HS256"],
                audience=os.getenv("JWT_AUDIENCE") or None,
                issuer=os.getenv("JWT_ISSUER") or None,
                options={"require": ["exp", "sub"], "verify_aud": bool(os.getenv("JWT_AUDIENCE"))},
            )
            return hashlib.sha256(f"{claims.get('iss', '')}:{claims['sub']}".encode()).hexdigest()[:20]
        except jwt.PyJWTError:
            return JSONResponse({"detail": "Invalid bearer token"}, status_code=401)

    if configured or jwt_public_key or jwt_secret:
        return JSONResponse({"detail": "Authentication required"}, status_code=401)
    return request.client.host if request.client else "unknown"


# ---------------------------------------------------------------------------
# FastAPI Lifespan & Application Setup
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(application: FastAPI):
    settings = Settings()
    registry = CatalogRegistry(settings.catalog_registry_path)
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, follow_redirects=True) as client:
        application.state.client = client
        application.state.registry = registry
        application.state.providers = provider_map(client, timeout=settings.request_timeout_seconds)
        application.state.service = CrossmatchService(
            registry,
            application.state.providers,
            radius_arcsec=settings.default_radius_arcsec,
            timeout=settings.request_timeout_seconds,
        )
        application.state.engine = DatasetEngine(
            registry_path=settings.catalog_registry_path,
            service=application.state.service,
        )
        application.state.metadata = application.state.engine.metadata
        application.state.tess_service = TessLightCurveService(os.getenv("TESS_CACHE_DIR"))
        yield
        del application.state.service
        del application.state.client
        del application.state.engine
        del application.state.metadata
        del application.state.tess_service
        await application.state.quota.close()


app = FastAPI(
    title="AstroSearch API",
    description="Astronomical catalog cross-matching and dataset generation service",
    version="0.2.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
    lifespan=lifespan,
)

cors_origins = [origin.strip() for origin in os.getenv("CORS_ORIGINS", "").split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins if cors_origins else ["*"],
    allow_credentials="*" not in (cors_origins or ["*"]),
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-AstroSearch-Retrieval-Status", "X-Request-ID"],
)

setup_metrics(app)
cache = CacheManager(os.getenv("REDIS_URL"))
app.state.quota = RequestQuota(os.getenv("REDIS_URL"))
_TESS_PLOT_LOCK = threading.Lock()


def get_service() -> CrossmatchService:
    service = getattr(app.state, "service", None)
    if service is None:
        settings = Settings()
        registry = CatalogRegistry(settings.catalog_registry_path)
        service = CrossmatchService(registry, provider_map(timeout=settings.request_timeout_seconds))
    return service


def get_engine() -> DatasetEngine:
    return getattr(app.state, "engine", None) or DatasetEngine()


def get_metadata() -> MetadataStore:
    return getattr(app.state, "metadata", None) or MetadataStore(
        local_dir=os.getenv("DATASET_STORAGE_PATH") or "datasets"
    )


# ---------------------------------------------------------------------------
# Middleware: Request Metrics, Quotas, and Body Limits
# ---------------------------------------------------------------------------


@app.middleware("http")
async def request_metrics(request: Request, call_next):
    started = time.monotonic()
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(request_id=request_id)
    endpoint = request.url.path
    metrics = getattr(app.state, "metrics", {})

    if "active_requests" in metrics:
        metrics["active_requests"].labels(endpoint="all").inc()
    try:
        is_cors_preflight = (
            request.method == "OPTIONS"
            and bool(request.headers.get("origin"))
            and bool(request.headers.get("access-control-request-method"))
        )
        if endpoint != "/api/v1/health" and not is_cors_preflight:
            identity = authenticate(request)
            if isinstance(identity, JSONResponse):
                response = identity
            elif not await app.state.quota.allow(identity, int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "60"))):
                response = JSONResponse(
                    {"detail": "Rate limit exceeded"}, status_code=429, headers={"Retry-After": "60"}
                )
            else:
                structlog.contextvars.bind_contextvars(actor=identity)
                maximum = int(os.getenv("MAX_REQUEST_BYTES", "1048576"))
                length = request.headers.get("content-length")
                if (length and int(length) > maximum) or len(await request.body()) > maximum:
                    response = JSONResponse({"detail": "Request body too large"}, status_code=413)
                else:
                    response = await call_next(request)
        else:
            response = await call_next(request)

        route = request.scope.get("route")
        endpoint_label = getattr(route, "path", endpoint if endpoint == "/api/v1/health" else "unmatched")
        response.headers["x-request-id"] = request_id
        logger.info(
            "request",
            method=request.method,
            path=endpoint_label,
            resource=request.url.path,
            status=response.status_code,
        )
        if "request_count" in metrics:
            metrics["request_count"].labels(
                method=request.method, endpoint=endpoint_label, status_code=str(response.status_code)
            ).inc()
        return response
    finally:
        if "active_requests" in metrics:
            metrics["active_requests"].labels(endpoint="all").dec()
        if "request_latency" in metrics:
            metrics["request_latency"].labels(
                method=request.method,
                endpoint=endpoint_label if "endpoint_label" in locals() else "unmatched",
            ).observe(time.monotonic() - started)
        structlog.contextvars.clear_contextvars()


# ---------------------------------------------------------------------------
# Pydantic Request & Response Schemas
# ---------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str = "healthy"
    timestamp: str
    version: str = "0.2.0"


class TargetRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    ra: float = Field(..., description="Right ascension in degrees", ge=0, lt=360)
    dec: float = Field(..., description="Declination in degrees", ge=-90, le=90)
    epoch: float | None = Field(None, ge=1800, le=2200, description="Julian epoch for proper motion correction")


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    ra: float | None = Field(None, description="Right ascension in degrees")
    dec: float | None = Field(None, description="Declination in degrees")
    name: str | None = Field(None, description="Astronomical object name (will be resolved)")
    radius_arcsec: float = Field(3.0, description="Search radius in arcseconds", gt=0)
    profile: str | None = Field(None, description="Catalog profile: optical, infrared, radio, etc.")
    epoch: float | None = Field(None, ge=1800, le=2200, description="Julian epoch for proper motion correction")
    object_types: list[str] | None = Field(None, description="Filter by object types")
    spectral_types: list[str] | None = Field(None, description="Filter by spectral classification")
    morphology: list[str] | None = None
    max_results: int | None = Field(None, gt=0, description="Maximum results per catalog")
    min_confidence: float = Field(0.5, description="Minimum match confidence", ge=0, le=1)
    catalogs: list[str] | None = None
    time_period: dict[str, Any] | None = None
    spatial_constraints: dict[str, Any] | None = None
    search_mode: Literal["cone", "shell", "cylinder"] = "cone"
    min_radius_arcsec: float = Field(0.0, ge=0)
    proper_motion: bool = True
    adaptive_radius: bool = False
    min_distance_pc: float | None = Field(None, gt=0)
    max_distance_pc: float | None = Field(None, gt=0)

    @field_validator("name")
    @classmethod
    def clean_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("name cannot be blank")
        return value

    @model_validator(mode="after")
    def validate_target_form(self):
        has_coordinates = self.ra is not None and self.dec is not None
        if (self.ra is None) != (self.dec is None):
            raise ValueError("ra and dec must be supplied together")
        if self.name is not None and has_coordinates:
            raise ValueError("provide either name or coordinates, not both")
        if self.name is None and not has_coordinates:
            raise ValueError("provide a name or both ra and dec")
        return self


class DatasetRequest(NormalizedDatasetRequest):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=100)
    profile: str = Field(..., description="Catalog profile to use")
    radius_arcsec: float = Field(10.0, description="Search radius in arcseconds", gt=0)
    object_types: list[str] | None = Field(None, description="Filter by object types")
    count_threshold: int = Field(5, description="Minimum detections per object", gt=0)
    time_period: dict[str, Any] | None = Field(None, description="Time period constraints")
    catalogs: list[str] | None = Field(None, description="Specific catalogs to query")
    filters: dict[str, Any] | None = Field(None, description="Additional filters")
    export_format: Literal["parquet", "csv", "fits", "json", "jsonl", "auto"] | None = None
    output_path: str | None = Field(None, description="Export path for the dataset")
    targets: list[dict[str, Any]] = Field(default_factory=list, description="Sky positions; count requests can traverse TAP")
    min_confidence: float = Field(0.0, ge=0, le=1)
    max_results: int | None = Field(None, gt=0)


    @model_validator(mode="after")
    def api_defaults(self):
        if "count_threshold" not in self.model_fields_set and self.count is not None:
            self.count_threshold = 1
        if self.output_format and self.export_format and self.export_format != self.output_format:
            raise ValueError("Conflicting output_format and export_format")
        for target in self.targets:
            TargetRequest.model_validate(target)
        return self


class SavedQueryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=100)
    query: SearchRequest


class SystemSummaryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(..., min_length=1, max_length=300)


class SignalObservationRequest(BaseModel):
    """A calibrated one-dimensional light curve or spectrum plus known references."""
    model_config = ConfigDict(extra="forbid")
    observation: dict[str, Any]
    references: list[dict[str, Any]] = Field(default_factory=list, max_length=10000)
    radius_arcsec: float = Field(2.0, gt=0, le=3600)
    representation_threshold: float = Field(0.25, ge=0, le=2)
    anomaly_threshold: float = Field(0.45, ge=0, le=2)


class TessTargetRequest(BaseModel):
    """Exactly one TESS target identifier form."""
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    tic_id: str | None = Field(None, min_length=1, max_length=32, description="TESS Input Catalog identifier")
    name: str | None = Field(None, min_length=1, max_length=300, description="Name resolved through CDS Sesame")
    ra_deg: float | None = Field(None, ge=0, lt=360)
    dec_deg: float | None = Field(None, ge=-90, le=90)

    @field_validator("tic_id")
    @classmethod
    def normalize_tic_id(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().upper().removeprefix("TIC").strip()
        if not normalized.isdigit():
            raise ValueError("tic_id must contain a bare numeric TIC identifier")
        return normalized

    @field_validator("name")
    @classmethod
    def clean_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("name cannot be blank")
        return value

    @model_validator(mode="after")
    def validate_target_form(self):
        has_coordinates = self.ra_deg is not None and self.dec_deg is not None
        if (self.ra_deg is None) != (self.dec_deg is None):
            raise ValueError("ra_deg and dec_deg must be supplied together")
        forms = int(self.tic_id is not None) + int(self.name is not None) + int(has_coordinates)
        if forms != 1:
            raise ValueError("provide exactly one of tic_id, name, or the ra_deg/dec_deg pair")
        return self


class TessLightCurveRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    target: TessTargetRequest
    sectors: list[int] | None = Field(None, min_length=1, max_length=100, description="Omit to retrieve all available sectors")
    flux_kind: Literal["SAP_FLUX", "PDCSAP_FLUX"] = "PDCSAP_FLUX"
    radius_arcsec: float = Field(5.0, gt=0, le=3600, description="Coordinate match radius")

    @field_validator("sectors")
    @classmethod
    def validate_sectors(cls, value: list[int] | None) -> list[int] | None:
        if value is None:
            return None
        if any(sector <= 0 for sector in value):
            raise ValueError("sector numbers must be positive")
        if len(value) != len(set(value)):
            raise ValueError("sectors must not contain duplicates")
        return value


class TessLightCurvePlotRequest(TessLightCurveRequest):
    normalize: bool = False
    show_uncertainties: bool = False
    quality_display: Literal["highlight", "hide"] = "highlight"
    period_days: float | None = Field(None, gt=0)
    epoch_btjd: float | None = None

    @model_validator(mode="after")
    def validate_phase_epoch(self):
        if self.epoch_btjd is not None and self.period_days is None:
            raise ValueError("epoch_btjd requires period_days")
        return self


@app.post("/api/v1/summaries/system")
async def system_summary_endpoint(req: SystemSummaryRequest):
    try:
        return await summarize_system(app.state.client, req.name)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ArchiveError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.post("/api/v1/summaries/object")
async def object_summary_endpoint(req: SearchRequest):
    try:
        return object_summary(await _search(req))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ObjectResolutionError as exc:
        code = 404 if "no coordinates found" in str(exc).lower() else 502
        raise HTTPException(status_code=code, detail=str(exc)) from exc


@app.post("/api/v1/signals/cross-reference")
async def signal_cross_reference_endpoint(req: SignalObservationRequest):
    """Represent a signal, rank known counterparts, and triage novelty for review."""
    try:
        return cross_reference_observation(
            req.observation, req.references, radius_arcsec=req.radius_arcsec,
            representation_threshold=req.representation_threshold, anomaly_threshold=req.anomaly_threshold,
        )
    except RepresentationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


def get_tess_service() -> TessLightCurveService:
    service = getattr(app.state, "tess_service", None)
    if service is None:
        service = TessLightCurveService(os.getenv("TESS_CACHE_DIR"))
    return service


async def _resolved_tess_target(target: TessTargetRequest) -> dict[str, Any]:
    if target.tic_id is not None:
        return {"tic_id": target.tic_id}
    if target.name is not None:
        client = getattr(app.state, "client", None)
        if client is None:
            raise TessServiceUnavailable("name resolution is unavailable outside the application lifespan")
        settings = Settings()
        resolved = await SesameResolver(client, endpoint=settings.resolver_endpoint).resolve(target.name)
        return {
            "name": target.name,
            "ra_deg": resolved.ra_deg,
            "dec_deg": resolved.dec_deg,
            "resolved_object": resolved.as_dict(),
        }
    return {"ra_deg": target.ra_deg, "dec_deg": target.dec_deg}


async def _retrieve_tess(req: TessLightCurveRequest) -> dict[str, Any]:
    try:
        target = await _resolved_tess_target(req.target)
        return await asyncio.to_thread(
            get_tess_service().retrieve,
            target,
            sectors=req.sectors,
            flux_kind=req.flux_kind,
            radius_arcsec=req.radius_arcsec,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ObjectResolutionError as exc:
        message = str(exc)
        lowered = message.lower()
        code = 404 if "no coordinates found" in lowered else 502
        raise HTTPException(status_code=code, detail=message) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except TessServiceUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except TessServiceError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def _render_tess_png(observations: list[dict[str, Any]], req: TessLightCurvePlotRequest, label: str) -> bytes:
    # Matplotlib's pyplot state is process-global; serialize figure construction.
    with _TESS_PLOT_LOCK:
        return plot_light_curves(
            observations,
            show_uncertainties=req.show_uncertainties,
            quality_display=req.quality_display,
            normalize=req.normalize,
            period_days=req.period_days,
            epoch_btjd=req.epoch_btjd,
            title=label,
        )


@app.post("/api/v1/signals/tess/light-curves")
async def retrieve_tess_light_curves(req: TessLightCurveRequest):
    """Search MAST for TESS SPOC light curves and return canonical observations."""
    result = await _retrieve_tess(req)
    if result["status"] == "failed":
        raise HTTPException(status_code=502, detail={"message": "all matching MAST products failed", **result})
    return result


@app.post("/api/v1/signals/tess/light-curves/plot")
async def plot_tess_light_curves(req: TessLightCurvePlotRequest):
    """Retrieve TESS SPOC light curves and render them as a PNG image."""
    result = await _retrieve_tess(req)
    if not result["observations"]:
        code = 502 if result["status"] == "failed" else 404
        detail = {"message": "no light curves could be plotted", **result}
        raise HTTPException(status_code=code, detail=detail)
    try:
        png = await asyncio.to_thread(
            _render_tess_png,
            result["observations"],
            req,
            f"TESS light curves — {req.target.tic_id or req.target.name or 'coordinate target'}",
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return Response(
        content=png,
        media_type="image/png",
        headers={"Content-Disposition": 'inline; filename="tess-light-curves.png"', "X-AstroSearch-Retrieval-Status": result["status"]},
    )


# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------


@app.get("/api/v1/health", response_model=HealthResponse)
async def health_check():
    """Service health check endpoint."""
    logger.info("health_check", endpoint="/api/v1/health")
    return HealthResponse(status="healthy", timestamp=datetime.now(UTC).isoformat())


@app.get("/api/v1/catalogs", response_model=dict[str, Any])
async def list_catalogs():
    """List all available astronomical catalogs."""
    logger.info("list_catalogs", endpoint="/api/v1/catalogs")
    service = get_service()
    return {name: asdict(catalog) for name, catalog in service.registry.catalogs.items()}


@app.get("/api/v1/catalogs/{catalog_name}", response_model=dict[str, Any])
async def get_catalog(catalog_name: str):
    """Retrieve detailed specification for a given catalog."""
    logger.info("get_catalog", endpoint="/api/v1/catalogs", catalog=catalog_name)
    service = get_service()
    try:
        catalog = service.registry.get(catalog_name)
        return asdict(catalog)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Catalog {catalog_name} not found")


async def _search(req: SearchRequest) -> dict[str, Any]:
    if not req.name and (req.ra is None or req.dec is None):
        raise ValueError("Either name or both ra and dec are required")

    cache_key = cache.make_key("search", req.model_dump())
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    resolved_info = None
    service = get_service()
    client = getattr(app.state, "client", None)

    if req.name:
        resolver = SesameResolver(client)
        resolved = await resolver.resolve(req.name)
        resolved_info = resolved.as_dict()
        ra, dec = resolved.ra_deg, resolved.dec_deg
    else:
        assert req.ra is not None and req.dec is not None
        ra, dec = req.ra, req.dec

    query = AdvancedQuery.from_dict({
        "ra": ra,
        "dec": dec,
        "epoch": req.epoch,
        "radius_arcsec": req.radius_arcsec,
        "profiles": [req.profile] if req.profile else None,
        "object_types": req.object_types,
        "spectral_types": req.spectral_types,
        "morphology": req.morphology,
        "min_confidence": req.min_confidence,
        "max_results": req.max_results,
        "catalogs": req.catalogs,
        "time_period": req.time_period,
        "spatial_constraints": req.spatial_constraints,
        "search_mode": req.search_mode,
        "min_radius_arcsec": req.min_radius_arcsec,
        "proper_motion": req.proper_motion,
        "adaptive_radius": req.adaptive_radius,
        "min_distance_pc": req.min_distance_pc,
        "max_distance_pc": req.max_distance_pc,
    })

    result = await service.crossmatch(ra, dec, query=query)
    if resolved_info:
        result.resolved_object = resolved_info

    data = result.as_dict()
    cache.set(cache_key, data, ttl=300)
    return data


@app.post("/api/v1/search", response_model=dict[str, Any])
async def search_endpoint(req: SearchRequest):
    """Execute single crossmatch search by coordinates or resolved object name."""
    logger.info("search_request", endpoint="/api/v1/search", ra=req.ra, dec=req.dec, name=req.name)
    try:
        return await _search(req)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error("search_error", error=str(e), endpoint="/api/v1/search")
        raise HTTPException(status_code=502, detail="Catalog search failed") from e


@app.post("/api/v1/search/batch", response_model=list[dict[str, Any]])
async def batch_search(requests: list[SearchRequest], max_concurrent: int = Query(10, ge=1, le=100)):
    """Execute multiple search queries concurrently with error isolation."""
    if len(requests) > int(os.getenv("API_MAX_BATCH_SIZE", "100")):
        raise HTTPException(status_code=413, detail="Batch contains too many searches")
    logger.info("batch_search_request", count=len(requests), endpoint="/api/v1/search/batch")
    semaphore = asyncio.Semaphore(max_concurrent)

    async def one(req: SearchRequest):
        try:
            async with semaphore:
                return await _search(req)
        except Exception as e:
            return {"error": str(e), "request": req.model_dump()}

    return await asyncio.gather(*(one(req) for req in requests))


@app.post("/api/v1/datasets/create", response_model=dict[str, Any], status_code=status.HTTP_202_ACCEPTED)
async def create_dataset_endpoint(req: DatasetRequest, background_tasks: BackgroundTasks):
    """Submit asynchronous dataset generation job."""
    if len(req.targets) > int(os.getenv("API_MAX_TARGETS", "1000")):
        raise HTTPException(status_code=413, detail="Dataset contains too many targets")

    engine = get_engine()
    try:
        req.validate_registry(engine.registry)
        selected_format = req.output_format or (None if req.export_format == "auto" else req.export_format)
        if req.count is None:
            selected_format = selected_format or "parquet"
        output_path = req.output_path or req.export_path
        if output_path:
            path = Path(output_path).resolve()
            if (
                not path.is_relative_to(engine.storage)
                or path.exists()
                or selected_format and path.suffix.lower() != f".{selected_format}"
            ):
                raise ValueError("output_path must be unused, inside DATASET_STORAGE_PATH, and match export_format")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    logger.info("create_dataset_request", name=req.name, profile=req.profile)
    try:
        payload = req.model_dump(exclude={"output_path", "export_format"})
        payload["output_format"] = selected_format
        payload["export_path"] = req.output_path or req.export_path
        metadata = enqueue_dataset(payload, engine)
        try:
            if not submit_to_redis(metadata["id"], payload):
                background_tasks.add_task(process_dataset_async, metadata["id"], payload, engine)
        except Exception as exc:
            metadata["status"] = "failed"
            metadata["error"] = "Job queue unavailable"
            engine.metadata.put_dataset(metadata)
            raise HTTPException(status_code=503, detail="Job queue unavailable") from exc
        return JSONResponse(metadata, status_code=202, headers={"Location": f"/api/v1/datasets/{metadata['id']}"})
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error("dataset_create_error", error=str(e), name=req.name)
        raise HTTPException(status_code=500, detail="Dataset creation failed") from e


@app.get("/api/v1/datasets", response_model=list[dict[str, Any]])
async def list_datasets_endpoint():
    """List all created datasets."""
    logger.info("list_datasets", endpoint="/api/v1/datasets")
    return get_engine().list_datasets()


@app.get("/api/v1/datasets/{dataset_name}", response_model=dict[str, Any])
async def get_dataset_endpoint(dataset_name: str):
    """Retrieve metadata and generation status of a dataset."""
    logger.info("get_dataset", dataset=dataset_name, endpoint="/api/v1/datasets")
    dataset = get_engine().get_dataset(dataset_name)
    if dataset:
        return dataset
    raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")


@app.get("/api/v1/datasets/{dataset_name}/export")
async def export_dataset_endpoint(dataset_name: str, partition: int | None = Query(None, ge=0)):
    """Download exported dataset file or stream from object storage."""
    engine = get_engine()
    dataset = engine.get_dataset(dataset_name)
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    if dataset["status"] not in {"completed", "partial"}:
        raise HTTPException(status_code=409, detail="Dataset export is not ready")

    if partition is not None:
        parts = dataset.get("partitions", [])
        if partition >= len(parts):
            raise HTTPException(status_code=404, detail="Partition not found")
        path = Path(parts[partition]["path"]).resolve()
        if not path.is_relative_to(engine.storage) or not path.is_file():
            raise HTTPException(status_code=404, detail="Partition file not found")
        return FileResponse(path, filename=path.name)
    if len(dataset.get("partitions", [])) > 1:
        return JSONResponse({"manifest": dataset["manifest_path"], "partitions": [
            {**p, "download_url": f"/api/v1/datasets/{dataset_name}/export?partition={i}"}
            for i, p in enumerate(dataset["partitions"])]})

    if dataset.get("export_uri"):
        body = engine.objects.get(dataset["export_uri"])

        def chunks():
            try:
                while chunk := body.read(1024 * 1024):
                    yield chunk
            finally:
                body.close()

        return StreamingResponse(
            chunks(),
            media_type="application/octet-stream",
            headers={"Content-Disposition": f'attachment; filename="{dataset_name}.{dataset["output_format"]}"'},
        )
    return FileResponse(dataset["export_path"], filename=f"{dataset_name}.{dataset['output_format']}")


@app.get("/api/v1/datasets/{dataset_name}/manifest")
async def dataset_manifest_endpoint(dataset_name: str):
    engine = get_engine()
    dataset = engine.get_dataset(dataset_name)
    if not dataset or not dataset.get("manifest_path"):
        raise HTTPException(status_code=404, detail="Manifest not found")
    path = Path(dataset["manifest_path"]).resolve()
    if not path.is_relative_to(engine.storage) or not path.is_file():
        raise HTTPException(status_code=404, detail="Manifest not found")
    return FileResponse(path, media_type="application/json", filename=path.name)


@app.delete("/api/v1/datasets/{dataset_name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset_endpoint(dataset_name: str):
    """Delete a dataset and its exported artifact."""
    logger.info("delete_dataset", dataset=dataset_name, endpoint="/api/v1/datasets")
    try:
        if get_engine().delete_dataset(dataset_name):
            return
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    raise HTTPException(status_code=404, detail=f"Dataset {dataset_name} not found")


@app.get("/api/v1/queries", response_model=list[dict[str, Any]])
async def list_queries_endpoint():
    """List all saved search queries."""
    return get_metadata().list_queries()


def _get_saved_query(query_id: str) -> dict[str, Any] | None:
    return next((item for item in get_metadata().list_queries() if item["id"] == query_id), None)


@app.get("/api/v1/queries/{query_id}", response_model=dict[str, Any])
async def get_query_endpoint(query_id: str):
    """Retrieve one saved search query, including its input definition."""
    query = _get_saved_query(query_id)
    if query is None:
        raise HTTPException(status_code=404, detail="Saved query not found")
    return query


@app.post("/api/v1/queries", response_model=dict[str, Any], status_code=201)
async def save_query_endpoint(req: SavedQueryRequest):
    """Save a search query for reuse."""
    return get_metadata().save_query(req.name, req.query.model_dump())


@app.post("/api/v1/queries/{query_id}/run", response_model=dict[str, Any])
async def run_saved_query_endpoint(query_id: str):
    """Execute a saved search query using the current catalog data sources."""
    saved = _get_saved_query(query_id)
    if saved is None:
        raise HTTPException(status_code=404, detail="Saved query not found")
    try:
        request = SearchRequest.model_validate(saved["query"])
        return await _search(request)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=f"Saved query is invalid: {exc}") from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("saved_query_run_error", query_id=query_id, error=str(exc))
        raise HTTPException(status_code=502, detail="Catalog search failed") from exc


@app.delete("/api/v1/queries/{query_id}", status_code=204)
async def delete_query_endpoint(query_id: str):
    """Delete a saved search query."""
    if not get_metadata().delete_query(query_id):
        raise HTTPException(status_code=404, detail="Saved query not found")


@app.get("/api/v1/stats", response_model=dict[str, Any])
async def stats_endpoint():
    """Retrieve service usage statistics."""
    datasets = get_engine().list_datasets()
    return {
        "datasets": len(datasets),
        "sources_exported": sum(item.get("total_sources", 0) for item in datasets),
        "saved_queries": len(get_metadata().list_queries()),
    }


@app.get("/api/v1/monitoring", response_model=dict[str, Any])
async def monitoring_endpoint():
    """Inspect provider circuit breaker states and service health."""
    service = getattr(app.state, "service", None)
    return {
        "status": "healthy",
        "provider_circuits": {
            endpoint: guard.state
            for provider in (service.providers.values() if service is not None else ())
            for endpoint, guard in getattr(provider, "guards", {}).items()
        },
    }


@app.get("/api/v1/monitoring/metrics")
async def metrics_endpoint():
    """Prometheus scrape endpoint."""
    logger.info("metrics_request", endpoint="/api/v1/monitoring/metrics")
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("unhandled_exception", error=str(exc), path=request.url.path)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})


def create_app() -> FastAPI:
    """Factory creating the configured FastAPI application instance."""
    return app
