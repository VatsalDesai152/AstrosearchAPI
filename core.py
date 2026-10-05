"""AstroSearch core: models, archive providers, query planning, and crossmatching."""



from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import json
import math
import os
import random
import statistics
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from xml.etree import ElementTree

import httpx
import yaml
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.io import ascii as astropy_ascii
from astropy.io.votable import parse as parse_votable
from astropy.time import Time
from dotenv import load_dotenv
from prometheus_client import Counter, Gauge, Histogram

local_env = Path.cwd() / ".env"
if not local_env.is_file():
    local_env = Path(__file__).resolve().parent / ".env"
load_dotenv(local_env, override=False)

# ===== data models and catalog registry =====
# ---------------------------------------------------------------------------
# Error Hierarchy
# ---------------------------------------------------------------------------


class AstroSearchError(Exception):
    """Base exception for all AstroSearch errors."""


class InvalidCoordinateError(AstroSearchError):
    """Raised when RA, DEC, or epoch coordinates fail validation."""


class CatalogUnavailableError(AstroSearchError):
    """Raised when an astronomy catalog or provider service is unavailable."""


class QueryTimeoutError(AstroSearchError):
    """Raised when a catalog query exceeds its allotted timeout."""


class CatalogQueryError(AstroSearchError):
    """Raised when an upstream catalog HTTP request returns an error."""


class ResponseParseError(AstroSearchError):
    """Raised when a provider response cannot be parsed."""


class ObjectResolutionError(AstroSearchError):
    """Raised when an astronomical object name cannot be resolved to coordinates."""


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class Target:
    """Canonical representation of a search target position."""

    ra: float
    dec: float
    frame: str = "icrs"
    epoch: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ResolvedObject:
    """Canonical identity and position returned by an object-name resolver."""

    query: str
    canonical_name: str | None
    ra_deg: float
    dec_deg: float
    aliases: list[str]
    object_type: str | None
    redshift: float | None
    pm_ra_masyr: float | None
    pm_dec_masyr: float | None
    epoch: float | None
    resolver: str
    resolver_metadata: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CatalogDefinition:
    """Specification of an astronomical catalog and its access parameters."""

    name: str
    provider: str
    wavelength: str
    enabled: bool = True
    endpoint: str | None = None
    table: str | None = None
    catalog: str | None = None
    description: str | None = None
    query_method: str | None = None
    units: str | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    profiles: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class CatalogSource:
    """A detected astronomical source returned by a catalog."""

    catalog: str
    source_id: str
    ra: float
    dec: float
    positional_error_arcsec: float | None
    data: dict[str, Any]
    metadata: dict[str, Any]
    provenance: dict[str, Any]
    epoch: float | None = None
    proper_motion_ra_masyr: float | None = None
    proper_motion_dec_masyr: float | None = None
    position_uncertainty_arcsec: float | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class QueryPlan:
    """Planned query for a single catalog."""

    catalog: str
    provider: str
    endpoint: str | None
    parameters: dict[str, Any]
    radius_arcsec: float
    wavelength: str = "unknown"

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Match:
    """Crossmatch association between a target and an archive detection."""

    catalog: str
    source: CatalogSource
    separation_arcsec: float
    confidence: float

    def as_dict(self) -> dict[str, Any]:
        return {
            "catalog": self.catalog,
            "source": self.source.as_dict(),
            "separation_arcsec": self.separation_arcsec,
            "confidence": self.confidence,
        }


@dataclass(slots=True)
class CatalogFailure:
    """Error record for a catalog that failed during execution."""

    catalog: str
    status: str = "failed"
    error_type: str | None = None
    message: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class UnifiedRecord:
    """Provenance-rich unified result aggregating all catalog crossmatches."""

    target: dict[str, Any]
    catalogs_queried: int
    catalog_results: dict[str, Any]
    counterparts: dict[str, list[dict[str, Any]]]
    failures: list[dict[str, Any]]
    provenance: dict[str, Any]
    crossmatch_groups: list[dict[str, Any]] = field(default_factory=list)
    resolved_object: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


# ---------------------------------------------------------------------------
# Runtime Settings
# ---------------------------------------------------------------------------


class Settings:
    """Runtime configuration read from environment variables or overrides."""

    def __init__(self, **overrides: Any) -> None:
        def val(name: str, default: Any) -> Any:
            return overrides.get(name, os.getenv(name, default))

        self.app_name = str(val("APP_NAME", "astro-crossmatch"))
        self.debug = str(val("DEBUG", "false")).lower() == "true"
        self.default_radius_arcsec = float(val("DEFAULT_RADIUS_ARCSEC", 3.0))
        self.request_timeout_seconds = float(val("REQUEST_TIMEOUT_SECONDS", 30.0))
        self.max_response_bytes = int(val("MAX_RESPONSE_BYTES", 10_000_000))
        self.resolver_endpoint = str(val("SESAME_ENDPOINT", "https://cds.unistra.fr/cgi-bin/nph-sesame/-oxp/SNV"))
        self.catalog_registry_path = val("CATALOG_REGISTRY_PATH", None)
        self.log_level = str(val("LOG_LEVEL", "INFO"))

        if not math.isfinite(self.default_radius_arcsec) or self.default_radius_arcsec <= 0:
            raise ValueError("DEFAULT_RADIUS_ARCSEC must be finite and greater than zero.")
        if not math.isfinite(self.request_timeout_seconds) or self.request_timeout_seconds <= 0:
            raise ValueError("REQUEST_TIMEOUT_SECONDS must be finite and greater than zero.")
        if self.max_response_bytes <= 0:
            raise ValueError("MAX_RESPONSE_BYTES must be greater than zero.")


# ---------------------------------------------------------------------------
# Coordinate Validation
# ---------------------------------------------------------------------------


def _as_float(value: Any, name: str) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise InvalidCoordinateError(f"{name} must be numeric.") from exc
    if not math.isfinite(numeric):
        raise InvalidCoordinateError(f"{name} must be finite.")
    return numeric


def validate_target(ra: float | str, dec: float | str, *, frame: str = "icrs", epoch: float | None = None) -> Target:
    """Validate coordinates and normalize right ascension to [0, 360)."""
    ra_val = _as_float(ra, "ra") % 360.0
    dec_val = _as_float(dec, "dec")
    if not -90.0 <= dec_val <= 90.0:
        raise InvalidCoordinateError("DEC must be within [-90, 90] degrees.")
    if epoch is not None:
        try:
            epoch = float(epoch)
        except (TypeError, ValueError) as exc:
            raise InvalidCoordinateError("epoch must be numeric.") from exc
        if not math.isfinite(epoch) or epoch < 1800 or epoch > 2200:
            raise InvalidCoordinateError("epoch must be a finite Julian year between 1800 and 2200.")
    try:
        coord = SkyCoord(ra=ra_val * u.deg, dec=dec_val * u.deg, frame=frame)
    except Exception as exc:
        raise InvalidCoordinateError(f"Unsupported coordinate frame: {frame}") from exc
    if not math.isfinite(coord.ra.deg) or not math.isfinite(coord.dec.deg):
        raise InvalidCoordinateError("Coordinate values are not finite.")
    return Target(ra=ra_val, dec=dec_val, frame=frame, epoch=epoch)


# ---------------------------------------------------------------------------
# Normalization & Parsers
# ---------------------------------------------------------------------------


def normalize_field_names(record: Mapping[str, Any]) -> dict[str, Any]:
    """Map heterogeneous astronomical catalog field names to canonical keys."""
    aliases = {
        "ra": "ra", "ra_icrs": "ra", "raj2000": "ra", "ramean": "ra",
        "dec": "dec", "dec_icrs": "dec", "dej2000": "dec", "decmean": "dec",
        "source_id": "source_id", "objid": "source_id", "designation": "source_id",
        "id": "source_id", "sourceid": "source_id", "main_id": "source_id",
        "objname": "source_id", "prefname": "source_id", "pl_name": "source_id", "oid": "source_id",
        "pmra": "pmra", "pm_ra": "pmra", "pmra_cosdec": "pmra",
        "pmdec": "pmdec", "pm_dec": "pmdec",
        "ref_epoch": "epoch", "epoch": "epoch", "obsepoch": "epoch",
        "poserr": "position_uncertainty_arcsec", "pos_error": "position_uncertainty_arcsec",
        "ra_error": "position_uncertainty_arcsec", "dec_error": "position_uncertainty_arcsec",
        "raerror": "position_uncertainty_arcsec", "decerror": "position_uncertainty_arcsec",
        "uncmaja": "position_uncertainty_arcsec", "err_pos": "position_uncertainty_arcsec",
        "parallax": "parallax", "plx_value": "parallax",
        "z": "redshift", "redshift": "redshift", "z_value": "redshift",
        "otype": "object_type", "objtype": "object_type", "prefphytype": "object_type",
        "morphology": "morphology", "sp_type": "spectral_type", "spectral_type": "spectral_type",
        "obsdate": "observation_date", "obs_date": "observation_date",
        "observation_date": "observation_date", "date_obs": "observation_date",
        "quality": "quality_flags", "quality_flag": "quality_flags", "flags": "quality_flags",
    }
    normalized: dict[str, Any] = {}
    for key, val in record.items():
        clean_key = str(key).strip().lower()
        normalized[aliases.get(clean_key, str(key).strip())] = val
    return normalized


def normalize_source_record(raw_record: Mapping[str, Any]) -> dict[str, Any]:
    """Canonicalize provider record values without hallucinating missing fields."""
    values = normalize_field_names(raw_record)
    if "ra" in values:
        try:
            values["ra"] = float(values["ra"])
        except (TypeError, ValueError):
            pass
    if "dec" in values:
        try:
            values["dec"] = float(values["dec"])
        except (TypeError, ValueError):
            pass
    if "source_id" in values:
        values["source_id"] = str(values["source_id"])
    for key in ("pmra", "pmdec", "epoch", "position_uncertainty_arcsec", "parallax", "redshift"):
        if key in values and values[key] not in (None, ""):
            try:
                values[key] = float(values[key])
            except (TypeError, ValueError):
                values.pop(key, None)
    return values


def parse_json_records(payload: str | bytes | dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    """Parse JSON records returned by public astronomy APIs."""
    data = json.loads(payload) if isinstance(payload, (str, bytes)) else payload
    if isinstance(data, dict):
        rows = data.get("data", data.get("results"))
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
        return [data]
    if isinstance(data, list):
        return [row for row in data if isinstance(row, dict)]
    return []


def parse_csv_records(payload: str | bytes) -> list[dict[str, Any]]:
    """Parse CSV text rows, skipping comment lines."""
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    csv_text = "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("#"))
    return [dict(row) for row in csv.DictReader(io.StringIO(csv_text))]


def parse_ipac_records(payload: str | bytes) -> list[dict[str, Any]]:
    """Parse an IPAC ASCII table (such as IRSA Gator responses) via Astropy."""
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    table = astropy_ascii.read(text, format="ipac")
    return [{name: row[name] for name in table.colnames} for row in table]


def parse_votable_records(payload: bytes | io.BytesIO | str) -> list[dict[str, Any]]:
    """Parse an IVOA VOTable XML stream via Astropy."""
    raw = payload if isinstance(payload, io.BytesIO) else io.BytesIO(payload.encode("utf-8") if isinstance(payload, str) else payload)
    table = parse_votable(raw)
    first_table = table.get_first_table()
    return [{key: val for key, val in zip(first_table.columns.names, row)} for row in first_table.array]


def build_provenance(
    catalog: str,
    *,
    provider: str,
    source_id: str,
    endpoint: str | None,
    query_parameters: Mapping[str, object],
    search_radius_arcsec: float,
) -> dict[str, object]:
    """Assemble complete provenance metadata for an archive observation."""
    return {
        "catalog": catalog,
        "provider": provider,
        "source_id": source_id,
        "retrieved_at": datetime.now(UTC).isoformat(),
        "endpoint": endpoint,
        "search_radius_arcsec": search_radius_arcsec,
        "query_parameters": dict(query_parameters),
    }


# ---------------------------------------------------------------------------
# Embedded Catalog Registry (16 definitions; 15 enabled by default)
# ---------------------------------------------------------------------------

DEFAULT_CATALOGS: dict[str, dict[str, Any]] = {
    "gaia_dr3": {
        "enabled": True,
        "provider": "tap",
        "wavelength": "optical",
        "endpoint": "https://gea.esac.esa.int/tap-server/tap/sync",
        "table": "gaiadr3.gaia_source",
        "description": "Gaia DR3 TAP positional and astrometric search",
        "query_method": "ADQL",
        "units": "degrees",
        "parameters": {"columns": ["source_id", "ra", "dec", "pmra", "pmdec", "ref_epoch", "parallax"]},
        "profiles": ["full", "optical", "stellar"],
    },
    "simbad": {
        "enabled": True,
        "provider": "tap",
        "wavelength": "multi",
        "endpoint": "https://simbad.cds.unistra.fr/simbad/sim-tap/sync",
        "table": "basic",
        "description": "SIMBAD astronomical object identities, types, and measurements",
        "query_method": "ADQL",
        "units": "degrees",
        "parameters": {
            "columns": ["main_id", "ra", "dec", "otype", "sp_type", "plx_value", "pmra", "pmdec"],
            "id_field": "main_id",
            "positional_error_field": "err_pos",
        },
        "profiles": ["full", "optical", "stellar", "identity"],
    },
    "ned": {
        "enabled": True,
        "provider": "tap",
        "wavelength": "extragalactic",
        "endpoint": "https://ned.ipac.caltech.edu/tap/sync",
        "table": "objdir",
        "description": "NASA/IPAC Extragalactic Database object directory",
        "query_method": "ADQL",
        "units": "degrees",
        "parameters": {
            "columns": ["prefname", "ra", "dec", "uncmaja", "z", "zflag", "prefphytype"],
            "id_field": "prefname",
            "positional_error_field": "uncmaja",
        },
        "profiles": ["full", "optical", "extragalactic", "identity"],
    },
    "exoplanet_archive": {
        "enabled": True,
        "provider": "tap",
        "wavelength": "exoplanet",
        "endpoint": "https://exoplanetarchive.ipac.caltech.edu/TAP/sync",
        "table": "ps",
        "description": "NASA Exoplanet Archive planetary systems",
        "query_method": "ADQL",
        "units": "degrees",
        "parameters": {
            "columns": ["pl_name", "hostname", "ra", "dec", "discoverymethod", "pl_orbper", "pl_rade"],
            "id_field": "pl_name",
        },
        "profiles": ["full", "exoplanet", "stellar"],
    },
    "vizier_2mass_reference": {
        "enabled": False,
        "provider": "tap",
        "wavelength": "infrared",
        "endpoint": "https://tapvizier.cds.unistra.fr/TAPVizieR/tap/sync",
        "table": "II/246/out",
        "description": "VizieR TAP 2MASS reference catalog",
        "query_method": "ADQL",
        "units": "degrees",
        "parameters": {
            "columns": ["2MASS", "RAJ2000", "DEJ2000", "Jmag", "Hmag", "Kmag"],
            "id_field": "2MASS",
            "ra_field": "RAJ2000",
            "dec_field": "DEJ2000",
        },
        "profiles": ["full", "infrared", "stellar"],
    },
    "twomass_psc": {
        "enabled": True,
        "provider": "irsa_gator",
        "wavelength": "infrared",
        "catalog": "fp_psc",
        "endpoint": "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query",
        "description": "2MASS Point Source Catalog",
        "query_method": "cone-search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "infrared", "stellar"],
    },
    "allwise": {
        "enabled": True,
        "provider": "irsa_gator",
        "wavelength": "infrared",
        "catalog": "allwise_p3as_psd",
        "endpoint": "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query",
        "description": "AllWISE mid-infrared source catalog",
        "query_method": "cone-search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "infrared", "stellar"],
    },
    "panstarrs_dr2": {
        "enabled": True,
        "provider": "mast",
        "wavelength": "optical",
        "endpoint": "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/mean",
        "description": "Pan-STARRS DR2 mean object catalog",
        "query_method": "positional API",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "optical", "extragalactic"],
    },
    "sdss": {
        "enabled": True,
        "provider": "sdss",
        "wavelength": "optical",
        "endpoint": "https://skyserver.sdss.org/dr18/SkyServerWS/ConeSearch/ConeSearchService",
        "description": "Sloan Digital Sky Survey cone search",
        "query_method": "cone-search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "optical", "extragalactic", "spectroscopy"],
    },
    "first": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "radio",
        "table": "first",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "FIRST radio catalogue (1.4 GHz)",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "radio", "extragalactic"],
    },
    "nvss": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "radio",
        "table": "nvss",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "NVSS radio catalogue (1.4 GHz all-sky)",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "radio", "extragalactic"],
    },
    "vlass": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "radio",
        "table": "vlass",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "VLASS radio catalogue (3 GHz)",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "radio", "extragalactic"],
    },
    "lotss": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "radio",
        "table": "lotss",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "LoTSS radio catalogue (120-168 MHz)",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "radio", "extragalactic"],
    },
    "rosat": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "xray",
        "table": "rosmaster",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "ROSAT all-sky survey master catalog",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "xray", "high-energy"],
    },
    "chandra": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "xray",
        "table": "chandra",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "Chandra source catalogue",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "xray", "high-energy"],
    },
    "xmm": {
        "enabled": True,
        "provider": "heasarc_xamin",
        "wavelength": "xray",
        "table": "xmm",
        "endpoint": "https://heasarc.gsfc.nasa.gov/xamin/query",
        "description": "XMM-Newton source catalogue",
        "query_method": "Xamin positional search",
        "units": "arcseconds",
        "parameters": {},
        "profiles": ["full", "xray", "high-energy"],
    },
}


class CatalogRegistry:
    """Catalog registry managing enabled astronomy catalogs with optional YAML override."""

    def __init__(self, registry_path: str | os.PathLike[str] | None = None) -> None:
        self.registry_path = Path(registry_path) if registry_path else None
        self._catalogs: dict[str, CatalogDefinition] = {}
        self.reload()

    def reload(self) -> None:
        """Load catalog definitions from YAML file if existing, or default embedded registry."""
        catalogs_data: dict[str, Any] = {}
        if self.registry_path and self.registry_path.exists():
            try:
                content = yaml.safe_load(self.registry_path.read_text(encoding="utf-8")) or {}
                catalogs_data = content.get("catalogs", {})
            except Exception:
                catalogs_data = DEFAULT_CATALOGS
        else:
            catalogs_data = DEFAULT_CATALOGS

        self._catalogs = {}
        for name, entry in catalogs_data.items():
            if not isinstance(entry, dict):
                continue
            self._catalogs[name] = CatalogDefinition(
                name=name,
                provider=str(entry.get("provider", "unknown")),
                wavelength=str(entry.get("wavelength", "unknown")),
                enabled=bool(entry.get("enabled", True)),
                endpoint=entry.get("endpoint"),
                table=entry.get("table"),
                catalog=entry.get("catalog"),
                description=entry.get("description"),
                query_method=entry.get("query_method"),
                units=entry.get("units"),
                parameters=dict(entry.get("parameters", {})),
                profiles=tuple(str(item) for item in entry.get("profiles", ())),
            )

    @property
    def catalogs(self) -> dict[str, CatalogDefinition]:
        return dict(self._catalogs)

    def enabled_catalogs(self) -> dict[str, CatalogDefinition]:
        return {name: cat for name, cat in self._catalogs.items() if cat.enabled}

    def get(self, name: str) -> CatalogDefinition:
        if name not in self._catalogs:
            raise KeyError(f"Catalog '{name}' not found in registry.")
        return self._catalogs[name]

    def by_profile(self, profile: str) -> dict[str, CatalogDefinition]:
        return {
            name: cat for name, cat in self.enabled_catalogs().items()
            if not cat.profiles or profile in cat.profiles
        }

# ===== archive providers and cache =====
# ---------------------------------------------------------------------------
# Resilience: Rate Limiter and Circuit Breaker
# ---------------------------------------------------------------------------

PROVIDER_REQUESTS = Counter("astrosearch_provider_requests_total", "Archive HTTP attempts", ["provider", "status"])
PROVIDER_SECONDS = Histogram("astrosearch_provider_seconds", "Archive HTTP latency", ["provider"])
PROVIDER_RETRIES = Counter("astrosearch_provider_retries_total", "Archive retries", ["provider"])
PROVIDER_ACTIVE = Gauge("astrosearch_provider_active", "Active archive requests in this process", ["provider"])


def provider_identity(endpoint: str | None, provider: str = "unknown") -> str:
    """Budget by archive rather than protocol (TAP serves several archives)."""
    host = urlparse(endpoint or "").hostname or ""
    for part, name in (("esa.int", "gaia"), ("stsci.edu", "mast"), ("irsa.ipac", "irsa"),
                       ("sdss.org", "sdss"), ("heasarc", "heasarc"), ("simbad", "simbad"),
                       ("exoplanetarchive", "exoplanet"), ("ned.ipac", "ned"), ("vizier", "vizier")):
        if part in host:
            return name
    return {"irsa_gator": "irsa", "heasarc_xamin": "heasarc"}.get(provider, provider.lower())


@dataclass(frozen=True)
class ProviderBudget:
    provider: str
    max_concurrency: int = 2
    requests_per_second: float = 1.0
    burst: int = 1
    backoff_base: float = 1.0
    backoff_max: float = 60.0
    lease_seconds: float = 120.0

    def __post_init__(self):
        values = (self.max_concurrency, self.requests_per_second, self.burst, self.backoff_base,
                  self.backoff_max, self.lease_seconds)
        if any(not math.isfinite(v) or v <= 0 for v in values):
            raise ValueError("Provider budgets must be positive and finite")

    @classmethod
    def configured(cls, provider: str) -> ProviderBudget:
        def value(key, default):
            return os.getenv(f"PROVIDER_{provider.upper()}_{key}", os.getenv(f"PROVIDER_{key}", str(default)))
        return cls(provider, int(value("MAX_CONCURRENCY", 2)), float(value("REQUESTS_PER_SECOND", 1)),
                   int(value("BURST", 1)), float(value("BACKOFF_BASE", 1)), float(value("BACKOFF_MAX", 60)),
                   float(value("LEASE_SECONDS", 120)))


def retry_after_seconds(value: str | None) -> float:
    try:
        delay = float(value or 0)
    except ValueError:
        try:
            delay = parsedate_to_datetime(value or "").timestamp() - time.time()
        except (ValueError, TypeError, OverflowError):
            return 0.0
    return max(0.0, delay) if math.isfinite(delay) else 0.0


# Atomic token bucket, expiring concurrency leases, and one half-open probe.
# Redis TIME avoids clock skew between workers. All keys share a cluster hash tag.
_ACQUIRE_BUDGET = """
local t = redis.call('TIME'); local now = t[1] + t[2]/1000000
local h = KEYS[1]; local leases = KEYS[2]
redis.call('ZREMRANGEBYSCORE', leases, '-inf', now)
local blocked = tonumber(redis.call('HGET', h, 'blocked') or '0')
if blocked > now then return tostring(blocked-now) end
local opened = tonumber(redis.call('HGET', h, 'opened') or '0')
if opened > 0 and redis.call('ZCARD', leases) > 0 then return '0.1' end
if redis.call('ZCARD', leases) >= tonumber(ARGV[3]) then return '0.05' end
local last = tonumber(redis.call('HGET', h, 'updated') or now)
local tokens = math.min(tonumber(ARGV[2]), tonumber(redis.call('HGET', h, 'tokens') or ARGV[2]) + (now-last)*ARGV[1])
redis.call('HSET', h, 'tokens', tokens, 'updated', now)
if tokens < 1 then return tostring((1-tokens)/ARGV[1]) end
redis.call('HSET', h, 'tokens', tokens-1)
redis.call('ZADD', leases, now+ARGV[4], ARGV[5])
redis.call('EXPIRE', leases, math.ceil(ARGV[4]*2))
return '0'
"""
_FAIL_BUDGET = """
local t = redis.call('TIME'); local now = t[1]+t[2]/1000000
local failures = redis.call('HINCRBY', KEYS[1], 'failures', 1)
local delay = tonumber(ARGV[1])
if failures >= tonumber(ARGV[2]) or tonumber(redis.call('HGET', KEYS[1], 'opened') or '0') > 0 then
  redis.call('HSET', KEYS[1], 'opened', now); delay = math.max(delay, tonumber(ARGV[3]))
end
local old = tonumber(redis.call('HGET', KEYS[1], 'blocked') or '0')
redis.call('HSET', KEYS[1], 'blocked', math.max(old, now+delay))
return failures
"""
_RENEW_BUDGET = """
local t = redis.call('TIME'); local now = t[1]+t[2]/1000000
if not redis.call('ZSCORE', KEYS[1], ARGV[1]) then return 0 end
redis.call('ZADD', KEYS[1], now+ARGV[2], ARGV[1])
redis.call('EXPIRE', KEYS[1], math.ceil(ARGV[2]*2)); return 1
"""


@dataclass
class EndpointGuard:
    """A fixed-interval request pacer combined with a three-state circuit breaker.

    One guard instance is shared per provider endpoint. It throttles request starts
    and transitions to 'open' when consecutive failure thresholds are exceeded.
    """

    requests_per_second: float = 1.0
    failure_threshold: int = 5
    recovery_seconds: float = 30.0
    clock: Callable[[], float] = time.monotonic
    _next_start: float = 0.0
    _failures: int = 0
    _opened_at: float | None = None
    _probe_in_flight: bool = False
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    budget: ProviderBudget | None = None
    redis_client: Any = None
    _mutex: Any = field(default_factory=threading.RLock)
    _tokens: float | None = None
    _updated: float = 0.0
    _active: int = 0
    _blocked_until: float = 0.0

    @property
    def redis_keys(self):
        name = self.budget.provider if self.budget else "unknown"
        return (f"astrosearch:budget:{{{name}}}", f"astrosearch:leases:{{{name}}}")

    async def _redis_call(self, method, *args):
        try:
            return await asyncio.to_thread(getattr(self.redis_client, method), *args)
        except Exception as exc:
            # Never replace a failed distributed limiter with independent budgets.
            raise CatalogUnavailableError("Shared provider budget unavailable; fetch deferred") from exc

    @asynccontextmanager
    async def slot(self):
        budget = self.budget or ProviderBudget("unknown", requests_per_second=self.requests_per_second)
        token = uuid.uuid4().hex
        while True:
            if self.redis_client is not None:
                delay = float(await self._redis_call("eval", _ACQUIRE_BUDGET, 2, *self.redis_keys,
                                                    budget.requests_per_second, budget.burst,
                                                    budget.max_concurrency, budget.lease_seconds, token))
            else:
                with self._mutex:
                    now = self.clock()
                    self._tokens = min(budget.burst, (self._tokens if self._tokens is not None else budget.burst)
                                       + max(0, now - self._updated) * budget.requests_per_second)
                    self._updated = now
                    delay = max(0, self._blocked_until - now)
                    if self._opened_at is not None:
                        delay = max(delay, self._opened_at + self.recovery_seconds - now)
                        if self._probe_in_flight:
                            delay = max(delay, 0.1)
                    if self._active >= budget.max_concurrency:
                        delay = max(delay, 0.05)
                    if self._tokens < 1:
                        delay = max(delay, (1 - self._tokens) / budget.requests_per_second)
                    if delay <= 0:
                        self._tokens -= 1
                        self._active += 1
                        self._probe_in_flight = self._opened_at is not None
            if delay <= 0:
                break
            await asyncio.sleep(min(delay, 30))

        async def renew():
            while True:
                await asyncio.sleep(budget.lease_seconds / 3)
                if not await self._redis_call("eval", _RENEW_BUDGET, 1, self.redis_keys[1], token, budget.lease_seconds):
                    raise CatalogUnavailableError("Provider lease expired")

        heartbeat = asyncio.create_task(renew()) if self.redis_client is not None else None
        owner = asyncio.current_task()
        if heartbeat:
            def lease_failed(task):
                if not task.cancelled() and task.exception() and owner:
                    owner.cancel()
            heartbeat.add_done_callback(lease_failed)
        PROVIDER_ACTIVE.labels(budget.provider).inc()
        try:
            yield
        finally:
            PROVIDER_ACTIVE.labels(budget.provider).dec()
            if heartbeat:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
            if self.redis_client is not None:
                await self._redis_call("zrem", self.redis_keys[1], token)
            else:
                with self._mutex:
                    self._active -= 1
                    self._probe_in_flight = False

    async def acquire(self) -> None:
        """Wait for rate limit interval and ensure the circuit is not open."""
        if self.requests_per_second <= 0:
            raise ValueError("requests_per_second must be positive")
        async with self._lock:
            now = self.clock()
            if self._opened_at is not None:
                if now - self._opened_at < self.recovery_seconds or self._probe_in_flight:
                    raise CatalogUnavailableError("Provider circuit is open")
                self._probe_in_flight = True
            delay = max(0.0, self._next_start - now)
            self._next_start = max(now, self._next_start) + 1.0 / self.requests_per_second
        if delay:
            await asyncio.sleep(delay)

    async def succeed(self) -> None:
        """Signal successful execution, resetting circuit breaker failures."""
        if self.redis_client is not None:
            await self._redis_call("hset", self.redis_keys[0], "failures", 0)
            await self._redis_call("hset", self.redis_keys[0], "opened", 0)
        with self._mutex:
            self._failures = 0
            self._opened_at = None
            self._probe_in_flight = False

    async def fail(self, delay: float = 0.0) -> None:
        """Record an endpoint failure and open the circuit if threshold is reached."""
        if self.redis_client is not None:
            await self._redis_call("eval", _FAIL_BUDGET, 1, self.redis_keys[0], delay,
                                  self.failure_threshold, self.recovery_seconds)
        with self._mutex:
            self._blocked_until = max(self._blocked_until, self.clock() + delay)
            self._failures += 1
            if self._probe_in_flight or self._failures >= self.failure_threshold:
                self._opened_at = self.clock()
            self._probe_in_flight = False

    @property
    def state(self) -> str:
        """Return the current circuit status: 'closed', 'half_open', or 'open'."""
        if self._opened_at is None:
            return "closed"
        if self.clock() - self._opened_at >= self.recovery_seconds:
            return "half_open"
        return "open"


_PROVIDER_GUARDS: dict[tuple, EndpointGuard] = {}
_GUARDS_LOCK = threading.Lock()


def provider_guard(provider: str) -> EndpointGuard:
    budget = ProviderBudget.configured(provider)
    url = os.getenv("REDIS_URL") or ""
    with _GUARDS_LOCK:
        key = (budget, url)
        if key not in _PROVIDER_GUARDS:
            connection = None
            if url:
                import redis
                connection = redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            _PROVIDER_GUARDS[key] = EndpointGuard(
                requests_per_second=budget.requests_per_second, budget=budget, redis_client=connection,
                failure_threshold=int(os.getenv("PROVIDER_FAILURE_THRESHOLD", "5")),
                recovery_seconds=float(os.getenv("PROVIDER_RECOVERY_SECONDS", "30")),
            )
        return _PROVIDER_GUARDS[key]


def budgeted_requests_adapter(provider: str):
    """Apply the same budgets to synchronous archive SDKs, at each HTTP attempt.

    Buffer one bounded response while holding its lease, including streamed downloads.
    SDK redirects pass through the adapter again and consume another token.
    """
    import requests

    class BudgetedAdapter(requests.adapters.HTTPAdapter):
        def send(self, request, **kwargs):
            original_send = super().send

            def transfer():
                kwargs["timeout"] = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "30"))
                response = original_send(request, **kwargs)
                try:
                    maximum = int(os.getenv("PROVIDER_PRODUCT_MAX_BYTES", str(128*1024*1024)))
                    body = bytearray()
                    for chunk in response.iter_content(1024*1024):
                        body.extend(chunk)
                        if len(body) > maximum:
                            raise CatalogQueryError("Archive product exceeded byte limit")
                    response._content = bytes(body)
                    response._content_consumed = True
                    return response
                finally:
                    response.close()

            async def guarded():
                guard = provider_guard(provider)
                budget = guard.budget or ProviderBudget.configured(provider)
                for attempt in range(3):
                    delay = min(budget.backoff_max, budget.backoff_base * 2**attempt) * random.uniform(0.5, 1)
                    async with guard.slot():
                        try:
                            with PROVIDER_SECONDS.labels(provider).time():
                                response = await asyncio.to_thread(transfer)
                            PROVIDER_REQUESTS.labels(provider, str(response.status_code)).inc()
                            if response.status_code not in {429, 500, 502, 503, 504}:
                                await guard.succeed()
                                return response
                            delay = max(delay, retry_after_seconds(response.headers.get("retry-after")))
                            await guard.fail(delay)
                            if attempt == 2:
                                return response
                        except requests.exceptions.RequestException:
                            PROVIDER_REQUESTS.labels(provider, "error").inc()
                            await guard.fail(delay)
                            if attempt == 2:
                                raise
                    PROVIDER_RETRIES.labels(provider).inc()
                    await asyncio.sleep(delay)
                raise CatalogQueryError("Archive SDK retry budget exhausted")

            return asyncio.run(guarded())

    return BudgetedAdapter(max_retries=0)


# ---------------------------------------------------------------------------
# Caching: Distributed and In-Memory Layer
# ---------------------------------------------------------------------------


class CacheManager:
    """Manages caching for provider responses and crossmatch queries."""

    def __init__(self, redis_url: str | None = None) -> None:
        self.redis_url = redis_url
        self._local_cache: dict[str, tuple[float, Any]] = {}
        self._redis = None
        if redis_url:
            try:
                import redis
                self._redis = redis.from_url(redis_url, socket_connect_timeout=2, socket_timeout=2)
            except Exception:
                self._redis = None

    def get(self, key: str) -> Any | None:
        """Retrieve cached value from Redis or local memory cache."""
        if self._redis:
            try:
                val = self._redis.get(key)
                if val:
                    return json.loads(val)
            except Exception:
                pass
        entry = self._local_cache.get(key)
        if entry and entry[0] > time.monotonic():
            return entry[1]
        self._local_cache.pop(key, None)
        return None

    def set(self, key: str, value: Any, ttl: int = 3600) -> None:
        """Store value with TTL in seconds."""
        if ttl <= 0:
            return
        self._local_cache[key] = (time.monotonic() + ttl, value)
        if self._redis:
            try:
                self._redis.setex(key, ttl, json.dumps(value, default=str))
            except Exception:
                pass

    def delete(self, key: str) -> None:
        """Invalidate cached entry."""
        self._local_cache.pop(key, None)
        if self._redis:
            try:
                self._redis.delete(key)
            except Exception:
                pass

    def clear(self) -> None:
        """Clear all cache keys."""
        self._local_cache.clear()
        if self._redis:
            try:
                keys = list(self._redis.scan_iter(match="astrosearch:cache:*"))
                if keys:
                    self._redis.delete(*keys)
            except Exception:
                pass

    @staticmethod
    def make_key(*args: Any) -> str:
        """Generate deterministic SHA-256 cache key."""
        payload = json.dumps(args, sort_keys=True)
        return "astrosearch:cache:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Base Catalog Provider
# ---------------------------------------------------------------------------


class CatalogProvider(ABC):
    """Abstract base class for astronomical archive providers."""

    @abstractmethod
    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        raise NotImplementedError


class _HTTPProvider(CatalogProvider):
    """Base HTTP provider managing connection pooling, exponential retry, and caching."""

    provider_name = "unknown"

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        timeout: float = 30.0,
        max_response_bytes: int = 10_000_000,
        guards: dict[str, EndpointGuard] | None = None,
        cache: CacheManager | None = None,
    ) -> None:
        self.client = client or httpx.AsyncClient(timeout=timeout, follow_redirects=True)
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self.guards = guards if guards is not None else {}
        self.cache = cache or CacheManager(os.getenv("REDIS_URL"))

    async def _get(self, endpoint: str, provider: str, **kwargs: Any) -> httpx.Response:
        """Execute GET with caching, rate limiting, and exponential retry on transient failures."""
        cache_key = self.cache.make_key("provider", endpoint, kwargs.get("params"))
        cached = self.cache.get(cache_key)
        if cached is not None:
            req = httpx.Request("GET", endpoint, params=kwargs.get("params"))
            return httpx.Response(
                cached["status"],
                headers={k: v for k, v in cached["headers"].items() if k.lower() not in {"content-encoding", "content-length"}},
                content=base64.b64decode(cached["content"]),
                request=req,
            )

        identity = provider_identity(endpoint, self.provider_name)
        guard = self.guards.setdefault(endpoint, provider_guard(identity))
        budget = guard.budget or ProviderBudget.configured(identity)
        last_error: Exception | None = None
        for attempt in range(3):
            delay = min(budget.backoff_max, budget.backoff_base * (2**attempt)) * random.uniform(0.5, 1.0)
            try:
                async with guard.slot():
                    with PROVIDER_SECONDS.labels(identity).time():
                        # Bound bytes during transfer, not after buffering a huge response.
                        async with self.client.stream("GET", endpoint, timeout=self.timeout, **kwargs) as streamed:
                            content = bytearray()
                            async for chunk in streamed.aiter_bytes():
                                content.extend(chunk)
                                if len(content) > self.max_response_bytes:
                                    raise CatalogQueryError(f"{provider} response exceeded byte limit")
                            headers = {k: v for k, v in streamed.headers.items() if k.lower() not in {"content-encoding", "content-length"}}
                            response = httpx.Response(streamed.status_code, headers=headers,
                                                      content=bytes(content), request=streamed.request)
                    PROVIDER_REQUESTS.labels(identity, str(response.status_code)).inc()
                    if response.status_code not in {429, 500, 502, 503, 504}:
                        await guard.succeed()
                        if response.status_code == 200:
                            self.cache.set(cache_key, {"status": response.status_code, "headers": dict(response.headers),
                                                      "content": base64.b64encode(response.content).decode()},
                                           ttl=int(os.getenv("PROVIDER_CACHE_TTL_SECONDS", "600")))
                        return response
                    last_error = CatalogQueryError(f"{provider} query failed: HTTP {response.status_code}")
                    delay = max(delay, retry_after_seconds(response.headers.get("retry-after")))
                    await guard.fail(delay)
            except (httpx.TransportError, TimeoutError) as exc:
                last_error = exc
                PROVIDER_REQUESTS.labels(identity, "timeout" if isinstance(exc, httpx.TimeoutException) else "error").inc()
                await guard.fail(delay)
            if attempt < 2:
                PROVIDER_RETRIES.labels(identity).inc()
                await asyncio.sleep(delay)
        raise CatalogQueryError(f"{provider} request failed after 3 attempts: {last_error}") from last_error

    def _sources(
        self,
        catalog: CatalogDefinition,
        rows: list[dict[str, Any]],
        radius_arcsec: float,
        endpoint: str | None,
        parameters: dict[str, Any],
        positional_error_key: str | None = None,
    ) -> list[CatalogSource]:
        """Convert heterogeneous provider rows into canonical CatalogSource objects."""
        sources: list[CatalogSource] = []
        for row in rows:
            try:
                normalized = normalize_source_record(row)
            except (TypeError, ValueError):
                continue
            if "ra" not in normalized or "dec" not in normalized:
                continue
            try:
                ra = float(str(normalized["ra"])) % 360.0
                dec = float(str(normalized["dec"]))
            except (TypeError, ValueError):
                continue
            if not math.isfinite(ra) or not math.isfinite(dec) or not -90.0 <= dec <= 90.0:
                continue

            # Position-based fallback is stable across pages/requests; row indices are not.
            source_id = str(normalized.get("source_id") or f"{catalog.name}:{ra:.10f}:{dec:.10f}")
            err_val = normalized.get("position_uncertainty_arcsec")
            if err_val is None and positional_error_key:
                err_val = row.get(positional_error_key)
            try:
                pos_err = float(str(err_val)) if err_val not in (None, "") else None
            except (TypeError, ValueError):
                pos_err = None

            sources.append(
                CatalogSource(
                    catalog=catalog.name,
                    source_id=source_id,
                    ra=ra,
                    dec=dec,
                    positional_error_arcsec=pos_err,
                    data=dict(row),
                    metadata={
                        "wavelength": catalog.wavelength,
                        "table": catalog.table,
                        "catalog": catalog.catalog,
                        "catalog_version": catalog.parameters.get("version"),
                        "physical": {
                            key: normalized[key]
                            for key in (
                                "parallax", "redshift", "object_type", "spectral_type",
                                "morphology", "observation_date", "quality_flags",
                            )
                            if key in normalized
                        },
                        "links": {
                            "SIMBAD": f"https://simbad.cds.unistra.fr/simbad/sim-id?Ident={quote(source_id)}"
                            if catalog.name == "simbad" else None,
                            "NED": f"https://ned.ipac.caltech.edu/byname?objname={quote(source_id)}"
                            if catalog.name == "ned" else None,
                            "MAST": f"https://mast.stsci.edu/portal/Mashup/Clients/Mast/Portal.html?searchQuery={ra}%20{dec}"
                            if catalog.provider == "mast" else None,
                            "IRSA": f"https://irsa.ipac.caltech.edu/applications/finderchart/servlet/api?locstr={ra}%20{dec}"
                            if catalog.provider == "irsa_gator" else None,
                            "LegacySurvey": f"https://www.legacysurvey.org/viewer/fits-cutout?ra={ra}&dec={dec}&pixscale=0.262&bands=griz"
                            if catalog.wavelength in {"optical", "extragalactic"} else None,
                        },
                    },
                    provenance=build_provenance(
                        catalog.name,
                        provider=self.provider_name,
                        source_id=source_id,
                        endpoint=endpoint,
                        query_parameters=parameters,
                        search_radius_arcsec=radius_arcsec,
                    ),
                    epoch=normalized.get("epoch"),
                    proper_motion_ra_masyr=normalized.get("pmra"),
                    proper_motion_dec_masyr=normalized.get("pmdec"),
                    position_uncertainty_arcsec=pos_err,
                )
            )
        return sources

    @staticmethod
    def _check(response: httpx.Response, provider: str) -> None:
        if response.status_code >= 400:
            raise CatalogQueryError(f"{provider} query failed: HTTP {response.status_code}")

    def _check_size(self, response: httpx.Response, provider: str) -> None:
        if len(response.content) > self.max_response_bytes:
            raise CatalogQueryError(f"{provider} response exceeded {self.max_response_bytes} byte limit.")


# ---------------------------------------------------------------------------
# Specific Archive Providers
# ---------------------------------------------------------------------------


class TapProvider(_HTTPProvider):
    """IVOA Table Access Protocol (TAP) adapter querying databases via ADQL."""

    provider_name = "tap"

    async def query_page(self, catalog: CatalogDefinition, target: Target | None, radius_arcsec: float,
                         *, limit: int, cursor: str | None = None, fields: list[str] | None = None):
        """Stable keyset pagination using the registry's unique source identifier."""
        import re
        params = dict(catalog.parameters)
        id_field = str(params.get("id_field", "source_id"))
        ra_field, dec_field = str(params.get("ra_field", "ra")), str(params.get("dec_field", "dec"))
        columns = list(params.get("columns") or [id_field, ra_field, dec_field])
        columns = list(dict.fromkeys([*(fields or columns), id_field, ra_field, dec_field]))
        for identifier in [*columns, id_field, ra_field, dec_field, catalog.table or ""]:
            if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9.]*', identifier):
                raise ValueError(f"Unsupported TAP identifier: {identifier}")
        conditions = []
        if target:
            conditions.append(f"CONTAINS(POINT('ICRS', {ra_field}, {dec_field}), "
                              f"CIRCLE('ICRS', {target.ra}, {target.dec}, {radius_arcsec / 3600})) = 1")
        if cursor is not None:
            # Numeric Gaia keys must stay integers (never round-trip through float).
            value = cursor if params.get("id_type", "integer" if catalog.name == "gaia_dr3" else "text") == "integer" else (
                "'" + cursor.replace("'", "''") + "'")
            if not value.startswith("'") and not re.fullmatch(r"[0-9]+", value):
                raise ValueError("Invalid numeric TAP cursor")
            conditions.append(f"{id_field} > {value}")
        adql = f"SELECT TOP {int(limit)} {', '.join(columns)} FROM {catalog.table}"
        if conditions:
            adql += " WHERE " + " AND ".join(conditions)
        adql += f" ORDER BY {id_field}"
        request = {"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "json", "QUERY": adql}
        if not catalog.endpoint:
            raise ValueError("Paged TAP queries require a registry endpoint")
        response = await self._get(catalog.endpoint, "TAP", params=request)
        self._check(response, "TAP")
        rows = self._parse_rows(response)
        if any(id_field not in row for row in rows):
            raise ResponseParseError("TAP page omitted its cursor column")
        next_cursor = str(rows[-1][id_field]) if len(rows) >= limit else None
        mapped = [dict(row, source_id=row[id_field], ra=row.get(ra_field), dec=row.get(dec_field)) for row in rows]
        return self._sources(catalog, mapped, radius_arcsec, catalog.endpoint, request), next_cursor

    @staticmethod
    def _parse_rows(response):
        ct = response.headers.get("content-type", "").lower()
        if "xml" in ct or "votable" in ct:
            if b'value="ERROR"' in response.content or b'value="OVERFLOW"' in response.content:
                raise ResponseParseError("TAP returned ERROR or OVERFLOW")
            return parse_votable_records(response.content)
        if "csv" in ct:
            return parse_csv_records(response.text)
        payload = response.json()
        if isinstance(payload, dict) and payload.get("metadata") and isinstance(payload.get("data"), list):
            names = [column["name"] for column in payload["metadata"]]
            return [dict(zip(names, row)) for row in payload["data"]]
        return parse_json_records(payload)

    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        endpoint = catalog.endpoint or "https://gea.esac.esa.int/tap-server/tap/sync"
        radius_deg = radius_arcsec / 3600.0
        params = catalog.parameters
        columns = params.get("columns") or ["source_id", "ra", "dec"]
        if isinstance(columns, str):
            columns = [c.strip() for c in columns.split(",") if c.strip()]
        columns = [str(col) for col in columns]

        ra_field = str(params.get("ra_field", "ra"))
        dec_field = str(params.get("dec_field", "dec"))
        id_field = str(params.get("id_field", "source_id"))
        pos_err_field = params.get("positional_error_field")

        for f in (ra_field, dec_field, id_field):
            if f not in columns:
                columns.append(f)

        adql = (
            "SELECT TOP 100 " + ", ".join(columns) + " FROM "
            f"{catalog.table or 'gaiadr3.gaia_source'} "
            "WHERE CONTAINS(POINT('ICRS', ra, dec), CIRCLE('ICRS', "
            f"{target.ra}, {target.dec}, {radius_deg})) = 1"
        )
        adql = adql.replace("POINT('ICRS', ra, dec)", f"POINT('ICRS', {ra_field}, {dec_field})")
        req_params = {"REQUEST": "doQuery", "LANG": "ADQL", "FORMAT": "json", "QUERY": adql}

        response = await self._get(endpoint, "TAP", params=req_params)
        self._check(response, "TAP")
        self._check_size(response, "TAP")

        try:
            rows = self._parse_rows(response)
        except Exception as exc:
            raise ResponseParseError(f"TAP response could not be parsed: {exc}") from exc

        mapped_rows = []
        for row in rows[:100]:
            mapped = dict(row)
            if ra_field in mapped:
                mapped["ra"] = mapped[ra_field]
            if dec_field in mapped:
                mapped["dec"] = mapped[dec_field]
            if id_field in mapped:
                mapped["source_id"] = mapped[id_field]
            mapped_rows.append(mapped)

        return self._sources(catalog, mapped_rows, radius_arcsec, endpoint, req_params, pos_err_field or "ra_error")


class IRSAGatorProvider(_HTTPProvider):
    """IRSA Gator cone search adapter (2MASS PSC, AllWISE)."""

    provider_name = "irsa_gator"

    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        endpoint = catalog.endpoint or "https://irsa.ipac.caltech.edu/cgi-bin/Gator/nph-query"
        params = {
            "catalog": catalog.catalog or catalog.name,
            "spatial": "cone",
            "objstr": f"{target.ra} {target.dec}",
            "radius": str(radius_arcsec),
            "radunits": "arcsec",
            "outfmt": "1",
        }
        response = await self._get(endpoint, "IRSA", params=params)
        self._check(response, "IRSA")
        self._check_size(response, "IRSA")
        try:
            rows = parse_ipac_records(response.content)
        except Exception as exc:
            raise ResponseParseError(f"IRSA response could not be parsed: {exc}") from exc
        return self._sources(catalog, rows, radius_arcsec, endpoint, params)


class MASTProvider(_HTTPProvider):
    """STScI MAST API adapter (Pan-STARRS DR2)."""

    provider_name = "mast"

    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        endpoint = catalog.endpoint or "https://catalogs.mast.stsci.edu/api/v0.1/panstarrs/dr2/mean"
        params = {"ra": str(target.ra), "dec": str(target.dec), "radius": str(radius_arcsec / 3600.0)}
        response = await self._get(endpoint, "MAST", params=params)
        self._check(response, "MAST")
        self._check_size(response, "MAST")
        try:
            rows = parse_json_records(response.json())
        except Exception as exc:
            raise ResponseParseError(f"MAST response could not be parsed: {exc}") from exc
        return self._sources(catalog, rows, radius_arcsec, endpoint, params, "raError")


class SDSSProvider(_HTTPProvider):
    """Sloan Digital Sky Survey (SDSS) SkyServer Cone Search adapter."""

    provider_name = "sdss"

    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        endpoint = catalog.endpoint or "https://skyserver.sdss.org/dr18/SkyServerWS/ConeSearch/ConeSearchService"
        params = {"format": "csv", "ra": str(target.ra), "dec": str(target.dec), "sr": str(radius_arcsec / 60.0)}
        response = await self._get(endpoint, "SDSS", params=params)
        self._check(response, "SDSS")
        self._check_size(response, "SDSS")
        try:
            rows = parse_csv_records(response.text)
        except Exception as exc:
            raise ResponseParseError(f"SDSS response could not be parsed: {exc}") from exc
        return self._sources(catalog, rows, radius_arcsec, endpoint, params)


class HEASARCXaminProvider(_HTTPProvider):
    """NASA HEASARC Xamin positional search adapter (FIRST, NVSS, ROSAT, Chandra, XMM)."""

    provider_name = "heasarc_xamin"

    async def query(self, catalog: CatalogDefinition, target: Target, radius_arcsec: float) -> list[CatalogSource]:
        endpoint = catalog.endpoint or "https://heasarc.gsfc.nasa.gov/xamin/query"
        params = {
            "table": catalog.table or catalog.name,
            "coord": f"{target.ra},{target.dec}",
            "radius": str(radius_arcsec),
            "format": "json",
        }
        response = await self._get(endpoint, "HEASARC Xamin", params=params)
        self._check(response, "HEASARC Xamin")
        self._check_size(response, "HEASARC Xamin")
        try:
            rows = parse_json_records(response.json())
        except Exception as exc:
            raise ResponseParseError(f"HEASARC Xamin response could not be parsed: {exc}") from exc
        return self._sources(catalog, rows, radius_arcsec, endpoint, params, "poserr")


# ---------------------------------------------------------------------------
# Object-Name Resolver: CDS Sesame
# ---------------------------------------------------------------------------


class SesameResolver:
    """Resolve astronomical object names through the CDS Sesame web service."""

    name = "cds_sesame"
    default_endpoint = "https://cds.unistra.fr/cgi-bin/nph-sesame/-oxp/SNV"

    def __init__(self, client: httpx.AsyncClient | None = None, *, endpoint: str | None = None) -> None:
        self.client = client or httpx.AsyncClient(timeout=30.0, follow_redirects=True)
        self.endpoint = endpoint or self.default_endpoint

    async def resolve(self, query: str) -> ResolvedObject:
        clean_query = str(query).strip()
        if not clean_query:
            raise ObjectResolutionError("Object name must not be empty.")
        url = f"{self.endpoint}?{quote(clean_query, safe='')}"
        try:
            response = await self.client.get(url, headers={"Accept": "application/xml, text/xml"})
        except httpx.HTTPError as exc:
            raise ObjectResolutionError(f"Sesame request failed: {exc}") from exc
        if response.status_code >= 400:
            raise ObjectResolutionError(f"Sesame request failed: HTTP {response.status_code}")
        try:
            return self.parse_response(clean_query, response.text, endpoint=self.endpoint)
        except (ElementTree.ParseError, ValueError, TypeError) as exc:
            raise ObjectResolutionError(f"Sesame response could not be parsed: {exc}") from exc

    @classmethod
    def parse_response(cls, query: str, payload: str, *, endpoint: str | None = None) -> ResolvedObject:
        root = ElementTree.fromstring(payload)
        records = [el for el in root.iter() if cls._local_name(el.tag) in {"result", "object"}]
        if not records:
            records = [root]

        for record in records:
            values: dict[str, list[str]] = {}
            for element in record.iter():
                key = cls._local_name(element.tag)
                val = (element.text or "").strip()
                if val:
                    values.setdefault(key, []).append(val)

            ra = cls._number(values, "jradeg", "ra", "ra_deg")
            dec = cls._number(values, "jdedeg", "dec", "dec_deg")
            if ra is None or dec is None:
                continue
            if not math.isfinite(ra) or not math.isfinite(dec) or not -90 <= dec <= 90:
                continue

            aliases = cls._all_values(values, "alias", "aliases", "oid")
            canonical = cls._first(values, "oname", "name", "canonical_name") or (aliases[0] if aliases else query)

            return ResolvedObject(
                query=query,
                canonical_name=canonical,
                ra_deg=ra % 360.0,
                dec_deg=dec,
                aliases=sorted({a for a in aliases if a != canonical}),
                object_type=cls._first(values, "otyp", "otype", "object_type"),
                redshift=cls._number(values, "z_value", "redshift", "z"),
                pm_ra_masyr=cls._number(values, "pmra", "pm_ra"),
                pm_dec_masyr=cls._number(values, "pmdec", "pm_dec"),
                epoch=cls._number(values, "epoch", "ref_epoch", "obsepoch"),
                resolver=cls.name,
                resolver_metadata={"endpoint": endpoint or cls.default_endpoint, "raw_fields": values},
            )
        raise ValueError(f"No coordinates found for object {query!r}.")

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1].strip().lower()

    @staticmethod
    def _first(values: dict[str, list[str]], *keys: str) -> str | None:
        for k in keys:
            if values.get(k):
                return values[k][0]
        return None

    @classmethod
    def _number(cls, values: dict[str, list[str]], *keys: str) -> float | None:
        val = cls._first(values, *keys)
        if val is None:
            return None
        try:
            res = float(val)
        except (TypeError, ValueError):
            return None
        return res if math.isfinite(res) else None

    @classmethod
    def _all_values(cls, values: dict[str, list[str]], *keys: str) -> list[str]:
        result: list[str] = []
        for k in keys:
            result.extend(values.get(k, []))
        return result


# ---------------------------------------------------------------------------
# Factory: Provider Map
# ---------------------------------------------------------------------------


def provider_map(
    client: httpx.AsyncClient | None = None,
    *,
    timeout: float | None = None,
    max_response_bytes: int | None = None,
    guards: dict[str, EndpointGuard] | None = None,
    cache: CacheManager | None = None,
) -> dict[str, CatalogProvider]:
    """Instantiate all catalog provider adapters with shared client, guards, and cache."""
    settings = Settings()
    shared_guards = guards if guards is not None else {}
    shared_cache = cache or CacheManager(os.getenv("REDIS_URL"))

    def make(cls: type[_HTTPProvider]) -> _HTTPProvider:
        return cls(
            client,
            timeout=timeout if timeout is not None else settings.request_timeout_seconds,
            max_response_bytes=max_response_bytes if max_response_bytes is not None else settings.max_response_bytes,
            guards=shared_guards,
            cache=shared_cache,
        )

    return {
        "tap": make(TapProvider),
        "irsa_gator": make(IRSAGatorProvider),
        "mast": make(MASTProvider),
        "sdss": make(SDSSProvider),
        "heasarc_xamin": make(HEASARCXaminProvider),
    }

# ===== query planning and crossmatching =====
# ---------------------------------------------------------------------------
# Advanced Query Representation
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class AdvancedQuery:
    """Extended query specification with astrophysical filters, geometric constraints, and limits."""

    target: Target
    radius_arcsec: float = 3.0
    profiles: list[str] | None = field(default_factory=list)
    object_types: list[str] | None = field(default_factory=list)
    spectral_types: list[str] | None = field(default_factory=list)
    morphology: list[str] | None = field(default_factory=list)
    count_threshold: int = 5
    min_confidence: float = 0.5
    max_results: int | None = None
    min_radius_arcsec: float = 0.0
    search_mode: str = "cone"
    spatial_constraints: dict[str, Any] = field(default_factory=dict)
    proper_motion: bool = True
    adaptive_radius: bool = False
    min_distance_pc: float | None = None
    max_distance_pc: float | None = None
    time_period: dict[str, Any] | None = field(default_factory=dict)
    filters: dict[str, Any] = field(default_factory=dict)
    catalogs: list[str] | None = field(default_factory=list)
    use_resolved_name: bool = False
    resolved_name: str | None = None
    export_format: str = "parquet"
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AdvancedQuery:
        """Construct an AdvancedQuery from a JSON dictionary payload."""
        target_data = data.get("target") or {}
        ra = target_data.get("ra", data.get("ra"))
        dec = target_data.get("dec", data.get("dec"))
        if ra is None or dec is None:
            raise ValueError("Coordinates (ra and dec) are required")

        target = validate_target(ra, dec, epoch=target_data.get("epoch", data.get("epoch")))
        return cls(
            target=target,
            radius_arcsec=float(data.get("radius_arcsec", 3.0)),
            profiles=data.get("profiles") or None,
            object_types=data.get("object_types") or None,
            spectral_types=data.get("spectral_types") or None,
            morphology=data.get("morphology") or None,
            count_threshold=int(data.get("count_threshold", 5)),
            min_confidence=float(data.get("min_confidence", 0.5)),
            max_results=int(data["max_results"]) if data.get("max_results") is not None else None,
            min_radius_arcsec=float(data.get("min_radius_arcsec", 0.0)),
            search_mode=str(data.get("search_mode", "cone")),
            spatial_constraints=data.get("spatial_constraints") or {},
            proper_motion=bool(data.get("proper_motion", True)),
            adaptive_radius=bool(data.get("adaptive_radius", False)),
            min_distance_pc=float(data["min_distance_pc"]) if data.get("min_distance_pc") is not None else None,
            max_distance_pc=float(data["max_distance_pc"]) if data.get("max_distance_pc") is not None else None,
            time_period=data.get("time_period") or None,
            filters=data.get("filters") or {},
            catalogs=data.get("catalogs") or None,
            use_resolved_name=bool(data.get("use_resolved_name", False)),
            resolved_name=data.get("resolved_name"),
            export_format=str(data.get("export_format", "parquet")),
            metadata=data.get("metadata") or {},
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert query into JSON-serializable representation."""
        return {
            "target": self.target.as_dict(),
            "radius_arcsec": self.radius_arcsec,
            "profiles": self.profiles,
            "object_types": self.object_types,
            "spectral_types": self.spectral_types,
            "morphology": self.morphology,
            "count_threshold": self.count_threshold,
            "min_confidence": self.min_confidence,
            "max_results": self.max_results,
            "min_radius_arcsec": self.min_radius_arcsec,
            "search_mode": self.search_mode,
            "spatial_constraints": self.spatial_constraints,
            "proper_motion": self.proper_motion,
            "adaptive_radius": self.adaptive_radius,
            "min_distance_pc": self.min_distance_pc,
            "max_distance_pc": self.max_distance_pc,
            "time_period": self.time_period,
            "filters": self.filters,
            "catalogs": self.catalogs,
            "use_resolved_name": self.use_resolved_name,
            "resolved_name": self.resolved_name,
            "export_format": self.export_format,
            "metadata": self.metadata,
        }

    def apply_filters(self, source: dict[str, Any]) -> bool:
        """Apply astrophysical, geometric, distance, and temporal constraints to a detection."""
        physical = source.get("physical") or source.get("metadata", {}).get("physical", {})

        # Object type filtering
        if self.object_types:
            obj_type = physical.get("object_type") or source.get("data", {}).get("object_type")
            if not obj_type or self._canonical_type(obj_type) not in {self._canonical_type(x) for x in self.object_types}:
                return False

        # Spectral type filtering
        if self.spectral_types:
            sp_type = physical.get("spectral_type") or source.get("data", {}).get("sp_type")
            if not sp_type or str(sp_type).casefold() not in {x.casefold() for x in self.spectral_types}:
                return False

        # Morphology filtering
        if self.morphology:
            morph = physical.get("morphology") or source.get("data", {}).get("morphology")
            if not morph or str(morph).casefold() not in {x.casefold() for x in self.morphology}:
                return False

        # Shell search min radius
        separation = source.get("separation_arcsec")
        if self.search_mode == "shell" and separation is not None and separation < self.min_radius_arcsec:
            return False

        # 3D Cylinder distance bounds via parallax inversion
        if self.search_mode == "cylinder":
            parallax = physical.get("parallax") or source.get("data", {}).get("parallax")
            dist = source.get("data", {}).get("distance_pc")
            try:
                distance_pc = float(dist) if dist is not None else 1000.0 / float(parallax)
            except (TypeError, ValueError, ZeroDivisionError):
                return False
            if distance_pc <= 0:
                return False
            if self.min_distance_pc is not None and distance_pc < self.min_distance_pc:
                return False
            if self.max_distance_pc is not None and distance_pc > self.max_distance_pc:
                return False

        # Spatial radius zones
        zones = self.spatial_constraints.get("radius_zones", [])
        if zones and separation is not None and not any(
            float(zone.get("min_arcsec", 0)) <= separation <= float(zone["max_arcsec"]) for zone in zones
        ):
            return False

        # Time period observation windows
        if self.time_period:
            observed = physical.get("observation_date") or source.get("data", {}).get("observation_date") or source.get("epoch")
            obs_year = self._year(observed)
            if obs_year is None:
                return False
            start = self._year(self.time_period.get("start_year", self.time_period.get("start")))
            end = self._year(self.time_period.get("end_year", self.time_period.get("end")))
            if start is not None and obs_year < start:
                return False
            if end is not None and obs_year > end:
                return False

        # Ray-casting exclusion polygons
        for polygon in self.spatial_constraints.get("exclude_polygons", []):
            if self._inside_polygon(float(source["ra"]), float(source["dec"]), polygon):
                return False

        return True

    @staticmethod
    def _canonical_type(value: Any) -> str:
        name = str(value).strip().casefold()
        return {
            "*": "star", "star": "star", "g": "galaxy", "galaxy": "galaxy",
            "qso": "quasar", "quasar": "quasar", "neb": "nebula",
            "nebula": "nebula", "cl*": "star_cluster", "star cluster": "star_cluster",
            "star_cluster": "star_cluster",
        }.get(name, name)

    @staticmethod
    def _year(value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, (date, datetime)):
            return float(value.year)
        try:
            return float(value)
        except (ValueError, TypeError):
            try:
                return float(datetime.fromisoformat(str(value)).year)
            except ValueError:
                return None

    @staticmethod
    def _inside_polygon(ra: float, dec: float, polygon: list[list[float]]) -> bool:
        """Ray-casting point-in-polygon containment handling 0/360 RA boundary crossing."""
        vertices = [(((float(x) - ra + 180) % 360) - 180, float(y)) for x, y in polygon]
        inside = False
        prev = vertices[-1]
        for curr in vertices:
            if (curr[1] > dec) != (prev[1] > dec):
                crossing = curr[0] + (dec - curr[1]) * (prev[0] - curr[0]) / (prev[1] - curr[1])
                if crossing > 0:
                    inside = not inside
            prev = curr
        return inside


# ---------------------------------------------------------------------------
# Query Validator & Builder
# ---------------------------------------------------------------------------


class QueryValidator:
    """Validates advanced search query constraints."""

    @staticmethod
    def validate(query: AdvancedQuery, registry: CatalogRegistry | None = None) -> bool:
        """Perform comprehensive constraint validation on an AdvancedQuery."""
        if not math.isfinite(query.radius_arcsec) or query.radius_arcsec <= 0:
            raise ValueError("radius_arcsec must be positive")
        if query.count_threshold <= 0:
            raise ValueError("count_threshold must be positive")
        if not math.isfinite(query.min_confidence) or not 0 <= query.min_confidence <= 1:
            raise ValueError("min_confidence must be between 0 and 1")
        if query.max_results is not None and query.max_results <= 0:
            raise ValueError("max_results must be positive")
        if query.search_mode not in {"cone", "shell", "cylinder"}:
            raise ValueError("search_mode must be cone, shell, or cylinder")
        if query.search_mode == "cylinder" and query.min_distance_pc is None and query.max_distance_pc is None:
            raise ValueError("cylinder searches require a distance bound")
        for dist in (query.min_distance_pc, query.max_distance_pc):
            if dist is not None and (not math.isfinite(dist) or dist <= 0):
                raise ValueError("distance bounds must be positive finite parsecs")
        if query.min_distance_pc is not None and query.max_distance_pc is not None and query.min_distance_pc > query.max_distance_pc:
            raise ValueError("min_distance_pc must not exceed max_distance_pc")
        if not math.isfinite(query.min_radius_arcsec) or query.min_radius_arcsec < 0 or query.min_radius_arcsec >= query.radius_arcsec:
            raise ValueError("min_radius_arcsec must be nonnegative and smaller than radius_arcsec")

        start = query._year((query.time_period or {}).get("start_year", (query.time_period or {}).get("start")))
        end = query._year((query.time_period or {}).get("end_year", (query.time_period or {}).get("end")))
        for k, v in (query.time_period or {}).items():
            if k in {"start_year", "end_year", "start", "end"} and v is not None and query._year(v) is None:
                raise ValueError(f"Invalid time_period {k}")
        if query.time_period and (start is None and end is None or start is not None and end is not None and start > end):
            raise ValueError("Invalid time_period")

        for poly in query.spatial_constraints.get("exclude_polygons", []):
            if not isinstance(poly, list) or len(poly) < 3 or any(not isinstance(pt, list) or len(pt) != 2 for pt in poly):
                raise ValueError("Each exclusion polygon needs at least three [ra, dec] vertices")
            if any(not all(math.isfinite(float(coord)) for coord in pt) or not -90 <= float(pt[1]) <= 90 for pt in poly):
                raise ValueError("Invalid exclusion polygon coordinate")

        for zone in query.spatial_constraints.get("radius_zones", []):
            try:
                inner = float(zone.get("min_arcsec", 0))
                outer = float(zone["max_arcsec"])
            except (TypeError, ValueError, KeyError, AttributeError) as exc:
                raise ValueError("Invalid radius zone") from exc
            if not math.isfinite(inner) or not math.isfinite(outer) or inner < 0 or outer > query.radius_arcsec or outer <= inner:
                raise ValueError("radius zones must lie within radius_arcsec")

        if query.catalogs:
            active_registry = registry or CatalogRegistry()
            for name in query.catalogs:
                if name not in active_registry.enabled_catalogs():
                    raise ValueError(f"Unknown catalog: {name}")
        return True


class QueryBuilder:
    """Builds catalog-specific query plans from an AdvancedQuery."""

    def __init__(self, registry: CatalogRegistry) -> None:
        self.registry = registry

    def build(self, query: AdvancedQuery) -> list[QueryPlan]:
        """Construct execution plans for all enabled catalogs matching the query profiles."""
        QueryValidator.validate(query, self.registry)
        plans = []
        for name, catalog in self.registry.enabled_catalogs().items():
            if query.catalogs and name not in query.catalogs:
                continue
            if query.profiles and not any(p in catalog.profiles for p in query.profiles):
                continue
            plans.append(
                QueryPlan(
                    catalog=name,
                    provider=catalog.provider,
                    endpoint=catalog.endpoint,
                    parameters={
                        "catalog": catalog.catalog,
                        "table": catalog.table,
                        **catalog.parameters,
                        "object_types": query.object_types,
                        "spectral_types": query.spectral_types,
                        "count_threshold": query.count_threshold,
                        "time_period": query.time_period,
                    },
                    radius_arcsec=query.radius_arcsec,
                    wavelength=catalog.wavelength,
                )
            )
        if not plans:
            raise ValueError("No catalogs match the requested profile and catalog selection")
        return plans


# ---------------------------------------------------------------------------
# Astrometric Geometry & Probabilistic Matching
# ---------------------------------------------------------------------------


def _source_coordinate(source: CatalogSource, epoch: float | None = None) -> SkyCoord:
    """Propagate catalog source coordinate to target epoch using proper motion."""
    coordinate = SkyCoord(ra=source.ra * u.deg, dec=source.dec * u.deg, frame="icrs")
    if (
        epoch is not None
        and source.epoch is not None
        and source.proper_motion_ra_masyr is not None
        and source.proper_motion_dec_masyr is not None
    ):
        try:
            moving = SkyCoord(
                ra=source.ra * u.deg,
                dec=source.dec * u.deg,
                pm_ra_cosdec=source.proper_motion_ra_masyr * u.mas / u.yr,
                pm_dec=source.proper_motion_dec_masyr * u.mas / u.yr,
                obstime=Time(source.epoch, format="jyear"),
                frame="icrs",
            )
            coordinate = moving.apply_space_motion(new_obstime=Time(epoch, format="jyear"))
        except (TypeError, ValueError, u.UnitConversionError):
            pass
    return coordinate


def angular_separation_arcsec(target: Target, source: CatalogSource | Target) -> float:
    """Compute spherical angular separation in arcseconds."""
    target_coord = SkyCoord(ra=target.ra * u.deg, dec=target.dec * u.deg, frame=target.frame)
    source_coord = (
        _source_coordinate(source, target.epoch)
        if isinstance(source, CatalogSource)
        else SkyCoord(ra=source.ra * u.deg, dec=source.dec * u.deg, frame="icrs")
    )
    return float(target_coord.separation(source_coord).to(u.arcsec).value)


def match_score(
    separation_arcsec: float,
    *,
    positional_error_arcsec: float | None = None,
    target_uncertainty_arcsec: float | None = None,
) -> float:
    """Calculate match confidence using Gaussian positional uncertainty quadrature."""
    if separation_arcsec < 0:
        return 0.0
    if positional_error_arcsec is not None or target_uncertainty_arcsec is not None:
        source_sigma = max(positional_error_arcsec or 0.0, 0.1)
        target_sigma = max(target_uncertainty_arcsec or 0.0, 0.0)
        sigma = math.sqrt(source_sigma**2 + target_sigma**2)
        likelihood = math.exp(-0.5 * (separation_arcsec / sigma) ** 2)
        return round(max(0.0, min(1.0, likelihood)), 6)
    scale = separation_arcsec / 3.0
    return round(max(0.0, 1.0 - min(scale, 10.0) / 10.0), 6)


def match_target(target: Target, sources: list[CatalogSource], radius_arcsec: float) -> list[Match]:
    """Filter sources by radius and rank by separation."""
    matches = []
    for source in sources:
        sep = angular_separation_arcsec(target, source)
        if sep <= radius_arcsec:
            matches.append(
                Match(
                    source.catalog,
                    source,
                    sep,
                    match_score(sep, positional_error_arcsec=source.positional_error_arcsec),
                )
            )
    return sorted(matches, key=lambda m: m.separation_arcsec)


def _source_dict(match: Match) -> dict[str, Any]:
    """Serialize a Match object into a comprehensive counterpart dictionary."""
    source = match.source
    return {
        "catalog": source.catalog,
        "source_id": source.source_id,
        "ra": source.ra,
        "dec": source.dec,
        "separation_arcsec": match.separation_arcsec,
        "confidence": match.confidence,
        "metadata": source.metadata,
        "data": source.data,
        "provenance": source.provenance,
        "positional_error_arcsec": source.positional_error_arcsec,
        "epoch": source.epoch,
        "proper_motion_ra_masyr": source.proper_motion_ra_masyr,
        "proper_motion_dec_masyr": source.proper_motion_dec_masyr,
        "physical": source.metadata.get("physical", {}),
        "links": {name: url for name, url in source.metadata.get("links", {}).items() if url},
    }


def _group_matches(matches: list[Match], target: Target, radius_arcsec: float) -> list[dict[str, Any]]:
    """Cluster multi-catalog detections into coherent physical objects using Disjoint-Set Union."""
    if not matches:
        return []
    parent = list(range(len(matches)))

    def find(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def union(i: int, j: int) -> None:
        root_i, root_j = find(i), find(j)
        if root_i != root_j:
            parent[root_j] = root_i

    for i in range(len(matches)):
        for j in range(i + 1, len(matches)):
            coord_i = _source_coordinate(matches[i].source, target.epoch)
            coord_j = _source_coordinate(matches[j].source, target.epoch)
            sep = float(coord_i.separation(coord_j).to(u.arcsec).value)
            allowed = max(
                radius_arcsec,
                matches[i].source.positional_error_arcsec or 0.0,
                matches[j].source.positional_error_arcsec or 0.0,
            )
            if sep <= allowed:
                union(i, j)

    grouped: dict[int, list[Match]] = {}
    for idx, match in enumerate(matches):
        grouped.setdefault(find(idx), []).append(match)

    result = []
    for num, group in enumerate(grouped.values(), start=1):
        result.append({
            "group_id": f"object-{num}",
            "catalogs": sorted({m.catalog for m in group}),
            "wavelengths": sorted({str(m.source.metadata.get("wavelength", "unknown")) for m in group}),
            "members": [_source_dict(m) for m in group],
        })
    return result


# ---------------------------------------------------------------------------
# Query Planner & Concurrent Executor
# ---------------------------------------------------------------------------


class QueryPlanner:
    """Creates default catalog query plans based on profile selections."""

    def __init__(self, registry: CatalogRegistry) -> None:
        self.registry = registry

    def plan(self, radius_arcsec: float, profile: str | None = None) -> list[QueryPlan]:
        plans = [
            QueryPlan(
                catalog=name,
                provider=catalog.provider,
                endpoint=catalog.endpoint,
                parameters={"catalog": catalog.catalog, "table": catalog.table, **catalog.parameters},
                radius_arcsec=radius_arcsec,
                wavelength=catalog.wavelength,
            )
            for name, catalog in self.registry.enabled_catalogs().items()
            if profile is None or not catalog.profiles or profile in catalog.profiles
        ]
        if not plans:
            raise ValueError("No catalogs match the requested profile")
        return plans


class QueryExecutor:
    """Executes catalog queries concurrently with timeouts and error isolation."""

    def __init__(self, providers: dict[str, CatalogProvider], *, timeout: float = 30.0) -> None:
        self.providers = providers
        self.timeout = timeout

    async def execute(
        self, plans: list[QueryPlan], target: Target
    ) -> tuple[list[tuple[str, list[CatalogSource]]], list[CatalogFailure]]:
        async def run(plan: QueryPlan) -> tuple[str, list[CatalogSource]]:
            provider = self.providers.get(plan.provider)
            if provider is None:
                raise CatalogUnavailableError(f"No provider configured for {plan.provider}")
            catalog = CatalogDefinition(
                name=plan.catalog,
                provider=plan.provider,
                wavelength=plan.wavelength,
                endpoint=plan.endpoint,
                table=plan.parameters.get("table"),
                catalog=plan.parameters.get("catalog"),
                parameters=dict(plan.parameters),
            )
            try:
                sources = await asyncio.wait_for(provider.query(catalog, target, plan.radius_arcsec), timeout=self.timeout)
                return plan.catalog, sources
            except TimeoutError as exc:
                raise QueryTimeoutError(f"Catalog {plan.catalog} timed out after {self.timeout:g}s") from exc

        gathered = await asyncio.gather(*(run(p) for p in plans), return_exceptions=True)
        successes: list[tuple[str, list[CatalogSource]]] = []
        failures: list[CatalogFailure] = []

        for plan, item in zip(plans, gathered):
            if isinstance(item, BaseException):
                failures.append(CatalogFailure(plan.catalog, error_type=item.__class__.__name__, message=str(item)))
            else:
                successes.append(item)
        return successes, failures


# ---------------------------------------------------------------------------
# Crossmatch Service
# ---------------------------------------------------------------------------


class CrossmatchService:
    """Orchestrates catalog querying, filtering, grouping, and UnifiedRecord assembly."""

    def __init__(
        self,
        registry: CatalogRegistry,
        providers: dict[str, CatalogProvider],
        *,
        radius_arcsec: float = 3.0,
        timeout: float = 30.0,
    ) -> None:
        self.registry = registry
        self.providers = providers
        self.radius_arcsec = radius_arcsec
        self.planner = QueryPlanner(registry)
        self.executor = QueryExecutor(providers, timeout=timeout)

    async def crossmatch(
        self,
        ra: float | str,
        dec: float | str,
        *,
        radius_arcsec: float | None = None,
        epoch: float | None = None,
        profile: str | None = None,
        query: AdvancedQuery | None = None,
    ) -> UnifiedRecord:
        """Execute full crossmatch pipeline for given coordinates or AdvancedQuery."""
        target = validate_target(ra, dec, epoch=epoch)
        if query is not None:
            QueryValidator.validate(query, self.registry)
            target = validate_target(
                query.target.ra,
                query.target.dec,
                epoch=query.target.epoch if query.proper_motion else None,
            )

        try:
            search_radius = (
                query.radius_arcsec if query else self.radius_arcsec if radius_arcsec is None else float(radius_arcsec)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("radius_arcsec must be a finite number greater than zero.") from exc
        if not math.isfinite(search_radius) or search_radius <= 0:
            raise ValueError("radius_arcsec must be a finite number greater than zero.")

        plans = QueryBuilder(self.registry).build(query) if query else self.planner.plan(search_radius, profile=profile)
        successes, failures = await self.executor.execute(plans, target)

        all_sources = [source for _, sources in successes for source in sources]
        matches = match_target(target, all_sources, search_radius)

        effective_radius = search_radius
        if query and query.adaptive_radius and matches:
            nearest = [m.separation_arcsec for m in matches[:5]]
            effective_radius = min(search_radius, max(1.0, 1.5 * statistics.median(nearest)))
            matches = [m for m in matches if m.separation_arcsec <= effective_radius]

        if query:
            counts: dict[str, int] = {}
            filtered: list[Match] = []
            for match in matches:
                if match.confidence < query.min_confidence or not query.apply_filters(_source_dict(match)):
                    continue
                c = counts.get(match.catalog, 0)
                if query.max_results is not None and c >= query.max_results:
                    continue
                counts[match.catalog] = c + 1
                filtered.append(match)
            matches = filtered

        allowed = {(m.catalog, m.source.source_id) for m in matches}
        catalog_results = {
            name: {
                "sources": [s for s in sources if (s.catalog, s.source_id) in allowed],
                "status": "success",
            }
            for name, sources in successes
        } if query else {name: {"sources": sources, "status": "success"} for name, sources in successes}

        counterparts: dict[str, list[dict[str, Any]]] = {}
        for match in matches:
            wave = str(match.source.metadata.get("wavelength", "unknown"))
            counterparts.setdefault(wave, []).append(_source_dict(match))

        failures_list = [f.as_dict() for f in failures]
        groups = _group_matches(matches, target, search_radius)

        provenance = {
            "query_radius_arcsec": search_radius,
            "effective_radius_arcsec": effective_radius,
            "target_epoch": target.epoch,
            "profile": profile,
            "advanced_query": query.to_dict() if query else None,
            "catalogs_planned": [p.catalog for p in plans],
            "matches": [
                {
                    "catalog": m.catalog,
                    "source_id": m.source.source_id,
                    "separation_arcsec": m.separation_arcsec,
                    "confidence": m.confidence,
                }
                for m in matches
            ],
        }

        return UnifiedRecord(
            target={"ra": target.ra, "dec": target.dec, "frame": target.frame},
            catalogs_queried=len(catalog_results) + len(failures_list),
            catalog_results=catalog_results,
            counterparts=counterparts,
            failures=failures_list,
            provenance=provenance,
            crossmatch_groups=groups,
        )

    async def crossmatch_many(
        self,
        targets: list[dict[str, Any]],
        *,
        radius_arcsec: float | None = None,
        epoch: float | None = None,
        profile: str | None = None,
    ) -> list[UnifiedRecord]:
        """Execute crossmatch pipeline sequentially or concurrently for multiple targets."""
        return [
            await self.crossmatch(
                t["ra"],
                t["dec"],
                radius_arcsec=t.get("radius_arcsec", radius_arcsec),
                epoch=t.get("epoch", epoch),
                profile=t.get("profile", profile),
            )
            for t in targets
        ]
