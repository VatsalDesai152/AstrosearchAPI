"""TESS FITS adaptation, MAST retrieval, caching, and command-line operations."""



from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import math
import os
import re
import sys
import threading
import uuid

import astropy.units as u
import httpx
import numpy as np
from astropy.coordinates import SkyCoord
from astropy.io import fits
from core import ObjectResolutionError, SesameResolver, Settings
from datetime import UTC, datetime
from pathlib import Path
from signals import RepresentationError, angular_separation_arcsec, plot_light_curves
from typing import Any
from urllib.parse import urlencode

# ===== FITS adaptation =====
ALLOWED_FLUX = {"SAP_FLUX", "PDCSAP_FLUX"}


def _json_numbers(values: np.ndarray) -> list[float | None]:
    """Preserve missing FITS samples as JSON null rather than non-standard NaN."""
    return [float(value) if np.isfinite(value) else None for value in values]


def read_tess_light_curve(
    path: Path,
    *,
    flux_kind: str = "PDCSAP_FLUX",
    source_url: str | None = None,
    product_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Read a SPOC light-curve product without erasing flags or provenance.

    ``product_metadata`` is supplied by archive retrieval. Calls that only pass
    a local FITS path keep the original identifier and behavior.
    """
    flux_kind = flux_kind.upper()
    if flux_kind not in ALLOWED_FLUX:
        raise RepresentationError(f"flux_kind must be one of {sorted(ALLOWED_FLUX)}")
    with fits.open(path, memmap=False) as hdus:
        if len(hdus) < 2 or hdus[1].data is None:
            raise RepresentationError("TESS product has no light-curve table")
        primary, table_header, data = hdus[0].header, hdus[1].header, hdus[1].data
        names = {name.upper() for name in data.names}
        error_name = flux_kind + "_ERR"
        required = {"TIME", flux_kind, error_name, "QUALITY"}
        if not required <= names:
            raise RepresentationError(f"TESS product lacks required columns: {sorted(required - names)}")
        tic_id = primary.get("TICID") or table_header.get("TICID") or primary.get("OBJECT") or path.stem
        sector = primary.get("SECTOR") or table_header.get("SECTOR")
        ra = primary.get("RA_OBJ", table_header.get("RA_OBJ"))
        dec = primary.get("DEC_OBJ", table_header.get("DEC_OBJ"))
        if ra is None or dec is None:
            raise RepresentationError("TESS product lacks RA_OBJ/DEC_OBJ")
        times = np.asarray(data["TIME"], dtype=float)
        values = np.asarray(data[flux_kind], dtype=float)
        errors = np.asarray(data[error_name], dtype=float)
        quality = np.asarray(data["QUALITY"], dtype=np.int64)
        if len(times) == 0:
            raise RepresentationError("TESS light-curve table contains no samples")
        metadata = dict(product_metadata or {})
        data_uri = metadata.get("data_uri") or metadata.get("dataURI")
        if data_uri:
            product_identity = f"{data_uri}\0{metadata.get('product_version') or metadata.get('prvversion') or ''}"
            product_key = hashlib.sha256(product_identity.encode("utf-8")).hexdigest()[:16]
            observation_id = f"tess-product-{product_key}-sector-{sector or 'unknown'}-tic-{tic_id}-{flux_kind.lower()}"
        else:
            observation_id = f"tess-sector-{sector or 'unknown'}-tic-{tic_id}-{flux_kind.lower()}"
        column_index = next(index + 1 for index, name in enumerate(data.names) if name.upper() == flux_kind)
        time_column_index = next(index + 1 for index, name in enumerate(data.names) if name.upper() == "TIME")
        time_unit = table_header.get(f"TUNIT{time_column_index}") or "d"
        time_format = table_header.get("TIMEFMT") or "BTJD"
        time_scale = table_header.get("TIMESYS") or primary.get("TIMESYS") or "TDB"
        bjdref = table_header.get("BJDREFI", primary.get("BJDREFI"))
        bjdreff = table_header.get("BJDREFF", primary.get("BJDREFF"))
        time_reference = None
        if bjdref is not None or bjdreff is not None:
            time_reference = float(bjdref or 0) + float(bjdreff or 0)
        value_unit = table_header.get(f"TUNIT{column_index}") or "electron / s"
        provenance = {
            "archive": metadata.get("archive", "MAST"),
            "local_product": path.name,
            "mission": primary.get("TELESCOP", "TESS"),
            "pipeline": primary.get("ORIGIN", "SPOC"),
            "pipeline_version": primary.get("PROCVER"),
            "sector": sector,
            "camera": primary.get("CAMERA"),
            "ccd": primary.get("CCD"),
            "axis_unit": time_unit,
            "time_format": time_format,
            "time_scale": time_scale,
            "value_unit": value_unit,
            "value_kind": flux_kind,
            "quality_policy": "QUALITY == 0",
        }
        if time_reference is not None:
            provenance["time_reference_bjd"] = time_reference
        time_zero = table_header.get("TIMEZERO", primary.get("TIMEZERO"))
        if time_zero is not None:
            provenance["time_zero"] = float(time_zero)
        metadata_fields = {
            "data_uri": ("data_uri", "dataURI"),
            "product_filename": ("product_filename", "productFilename"),
            "product_version": ("product_version", "productVersion", "prvversion"),
            "mast_obs_id": ("mast_obs_id", "obs_id"),
            "mast_obsid": ("mast_obsid", "obsid"),
            "sequence_number": ("sequence_number",),
            "retrieved_at": ("retrieved_at",),
            "product_url": ("product_url", "source_url"),
            "cache_path": ("cache_path",),
        }
        for output_name, candidates in metadata_fields.items():
            for candidate in candidates:
                if metadata.get(candidate) is not None:
                    provenance[output_name] = metadata[candidate]
                    break
        provenance["tic_id"] = str(tic_id)
        provenance["target_ra_deg"] = float(ra)
        provenance["target_dec_deg"] = float(dec)
        return {
            "observation_id": observation_id,
            "object_id": f"TIC {tic_id}",
            "title": f"TIC {tic_id}, TESS sector {sector or 'unknown'} ({flux_kind})",
            "modality": "light_curve",
            "ra_deg": float(ra),
            "dec_deg": float(dec),
            "axis": _json_numbers(times),
            "values": _json_numbers(values),
            "uncertainties": _json_numbers(errors),
            "quality": quality.tolist(),
            "instrument": "TESS",
            "band": "TESS",
            "observed_at": primary.get("DATE-OBS") or table_header.get("DATE-OBS"),
            "source_url": source_url or metadata.get("product_url", ""),
            "provenance": provenance,
        }

# ===== MAST retrieval and cache =====
logger = logging.getLogger(__name__)


class TessServiceError(RuntimeError):
    """The archive query could not be completed."""


class TessServiceUnavailable(TessServiceError):
    """Astroquery is not installed or the archive client is unavailable."""


def _value(row: Any, name: str, default: Any = None) -> Any:
    """Read an Astropy row field without leaking masked values into JSON."""
    try:
        value = row[name]
    except (KeyError, IndexError, TypeError, ValueError):
        return default
    if np.ma.is_masked(value):
        return default
    if value is None:
        return default
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, float) and not np.isfinite(value):
        return default
    if isinstance(value, str) and value.strip() in {"", "--", "nan", "NaN"}:
        return default
    return value


def _field_name(table: Any, name: str) -> str | None:
    try:
        return next((column for column in table.colnames if column.casefold() == name.casefold()), None)
    except AttributeError:
        return None


def _get(row: Any, name: str, default: Any = None) -> Any:
    table = getattr(row, "table", None)
    actual = _field_name(table, name) if table is not None else name
    if actual is None:
        return default
    return _value(row, actual, default)


def _text(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value).strip()


def _canonical_tic(value: Any) -> str | None:
    match = re.search(r"(?:TIC\s*)?(\d+)", _text(value), flags=re.IGNORECASE)
    return match.group(1) if match else None


def _mast_source_url(data_uri: str) -> str:
    return "https://mast.stsci.edu/api/v0.1/Download/file?" + urlencode({"uri": data_uri})


def _safe_filename(filename: str, data_uri: str) -> str:
    basename = Path(filename.replace("\\", "/")).name
    if not basename or basename in {".", ".."}:
        basename = Path(data_uri.rsplit("/", 1)[-1]).name or "tess-light-curve.fits"
    return re.sub(r"[^A-Za-z0-9._-]+", "_", basename)


class TessLightCurveService:
    """Retrieve public SPOC light curves from MAST with a durable FITS cache.

    ``observations`` can be an Astroquery-compatible client object, which also
    makes the archive boundary replaceable by an offline fake.
    """

    def __init__(self, cache_dir: str | Path | None = None, *, observations: Any | None = None) -> None:
        configured = cache_dir or os.getenv("TESS_CACHE_DIR")
        self.cache_dir = Path(configured).expanduser() if configured else Path.home() / ".cache" / "astrosearch" / "tess"
        self.cache_dir = self.cache_dir.resolve()
        self._observations = observations
        self._archive_lock = threading.Lock()
        self._product_locks: dict[str, threading.Lock] = {}
        self._product_locks_guard = threading.Lock()

    @property
    def observations(self) -> Any:
        if self._observations is None:
            try:
                from astroquery.mast import Observations
            except ImportError as exc:
                raise TessServiceUnavailable("TESS retrieval requires astroquery; install the AstroSearch project dependencies") from exc
            self._observations = Observations
        return self._observations

    def _query_observations(
        self,
        target: dict[str, Any],
        *,
        sectors: list[int] | None,
        radius_arcsec: float,
    ) -> tuple[Any, float | None, float | None]:
        tic_id = _canonical_tic(target.get("tic_id"))
        coordinate = None
        if tic_id:
            criteria: dict[str, Any] = {
                "obs_collection": "TESS",
                "provenance_name": "SPOC",
                "dataproduct_type": "timeseries",
                "target_name": tic_id,
            }
            if sectors:
                criteria["sequence_number"] = sectors if len(sectors) > 1 else sectors[0]
            with self._archive_lock:
                observations = self.observations.query_criteria(**criteria)
            target_ra = target_dec = None
        else:
            try:
                target_ra, target_dec = float(target["ra_deg"]), float(target["dec_deg"])
                if not np.isfinite(target_ra) or not np.isfinite(target_dec) or not 0 <= target_ra < 360 or not -90 <= target_dec <= 90:
                    raise ValueError("coordinates must satisfy 0 <= RA < 360 and -90 <= Dec <= 90")
                coordinate = SkyCoord(ra=target_ra * u.deg, dec=target_dec * u.deg, frame="icrs")
            except (KeyError, TypeError, ValueError) as exc:
                if isinstance(exc, ValueError) and str(exc).startswith("coordinates must"):
                    raise
                raise ValueError("target must contain valid tic_id or both ra_deg and dec_deg") from exc
            with self._archive_lock:
                observations = self.observations.query_region(coordinate, radius=radius_arcsec * u.arcsec)

        selected = []
        for row in observations:
            if _text(_get(row, "obs_collection")).casefold() != "tess":
                continue
            if _text(_get(row, "provenance_name")).casefold() != "spoc":
                continue
            if _text(_get(row, "dataproduct_type")).casefold() != "timeseries":
                continue
            if tic_id and _canonical_tic(_get(row, "target_name")) != tic_id:
                continue
            if sectors:
                try:
                    if int(_get(row, "sequence_number")) not in sectors:
                        continue
                except (TypeError, ValueError):
                    continue
            selected.append(True)

        # Preserve Astroquery's table type for get_product_list while applying
        # explicit in-process guards in case archive filters are broadened.
        if len(selected) == len(observations):
            filtered_observations = observations
        elif selected:
            # Row indices are positions in the returned table, not MAST obs IDs.
            mask = []
            for row in observations:
                keep = (
                    _text(_get(row, "obs_collection")).casefold() == "tess"
                    and _text(_get(row, "provenance_name")).casefold() == "spoc"
                    and _text(_get(row, "dataproduct_type")).casefold() == "timeseries"
                )
                if tic_id:
                    keep = keep and _canonical_tic(_get(row, "target_name")) == tic_id
                if sectors:
                    try:
                        keep = keep and int(_get(row, "sequence_number")) in sectors
                    except (TypeError, ValueError):
                        keep = False
                mask.append(keep)
            filtered_observations = observations[mask]
        else:
            filtered_observations = observations[:0]
        return filtered_observations, target_ra if not tic_id else None, target_dec if not tic_id else None

    def _product_lock(self, key: str) -> threading.Lock:
        with self._product_locks_guard:
            return self._product_locks.setdefault(key, threading.Lock())

    @staticmethod
    def _valid_fits(path: Path) -> bool:
        if not path.is_file() or path.stat().st_size < 2880:
            return False
        try:
            from astropy.io import fits

            with fits.open(path, memmap=False, checksum=True) as hdus:
                return len(hdus) >= 2 and hdus[1].data is not None
        except Exception:
            return False

    def _download_product(
        self, data_uri: str, filename: str, product_version: Any = None
    ) -> tuple[Path, bool, str]:
        cache_identity = f"{data_uri}\0{product_version or ''}"
        key = hashlib.sha256(cache_identity.encode("utf-8")).hexdigest()
        safe_name = _safe_filename(filename, data_uri)
        directory = self.cache_dir / "products" / key[:2]
        destination = directory / f"{key[:20]}-{safe_name}"
        directory.mkdir(parents=True, exist_ok=True)
        with self._product_lock(key):
            if self._valid_fits(destination):
                return destination, True, str(destination.relative_to(self.cache_dir))
            if destination.exists():
                destination.unlink()
            temporary = destination.with_name(destination.name + f".{threading.get_ident()}.part")
            temporary.unlink(missing_ok=True)
            try:
                with self._archive_lock:
                    result = self.observations.download_file(
                        data_uri,
                        local_path=str(temporary),
                        cache=True,
                        verbose=False,
                    )
                status = result[0] if isinstance(result, tuple) and result else result
                if _text(status).upper() != "COMPLETE":
                    detail = result[1] if isinstance(result, tuple) and len(result) > 1 else status
                    raise RuntimeError(f"MAST download status {_text(status, 'unknown')}: {_text(detail)}")
                if not self._valid_fits(temporary):
                    raise RuntimeError("MAST returned a file that is not a readable TESS light-curve FITS product")
                temporary.replace(destination)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
            return destination, False, str(destination.relative_to(self.cache_dir))

    @staticmethod
    def _failure(product: dict[str, Any], error_type: str, message: str) -> dict[str, Any]:
        return {
            "sector": product.get("sequence_number"),
            "product_id": product.get("data_uri"),
            "error_type": error_type,
            "message": message[:500],
        }

    def retrieve(
        self,
        target: dict[str, Any],
        *,
        sectors: list[int] | None = None,
        flux_kind: str = "PDCSAP_FLUX",
        radius_arcsec: float = 5.0,
    ) -> dict[str, Any]:
        """Return canonical observations and structured per-product outcomes."""
        flux_kind = flux_kind.upper()
        if flux_kind not in {"SAP_FLUX", "PDCSAP_FLUX"}:
            raise ValueError("flux_kind must be SAP_FLUX or PDCSAP_FLUX")
        if not np.isfinite(radius_arcsec) or radius_arcsec <= 0 or radius_arcsec > 3600:
            raise ValueError("radius_arcsec must be finite and between 0 and 3600 arcseconds")
        if sectors is not None:
            sectors = sorted(set(int(sector) for sector in sectors))
            if not sectors:
                sectors = None
            elif any(sector <= 0 for sector in sectors):
                raise ValueError("sector numbers must be positive")

        retrieved_at = datetime.now(UTC).isoformat()
        tic_id = _canonical_tic(target.get("tic_id"))
        try:
            observations, target_ra, target_dec = self._query_observations(
                target, sectors=sectors, radius_arcsec=radius_arcsec
            )
            if len(observations) == 0:
                return self._result("no_products", target, sectors, flux_kind, radius_arcsec, [], [], 0, retrieved_at)
            # get_product_list must receive the observation rows/table; mission
            # obs_id strings are deliberately never substituted for group obsid.
            with self._archive_lock:
                all_products = self.observations.get_product_list(observations)
                light_curves = self.observations.filter_products(
                    all_products,
                    productSubGroupDescription="LC",
                    productType="SCIENCE",
                    extension="fits",
                )
        except TessServiceError:
            raise
        except Exception as exc:
            raise TessServiceError(f"MAST TESS observation or product query failed: {exc}") from exc

        product_rows: dict[str, Any] = {}
        for row in light_curves:
            data_uri = _text(_get(row, "dataURI"))
            if data_uri:
                product_rows.setdefault(data_uri, row)
        products = sorted(
            product_rows.values(),
            key=lambda row: (
                int(_get(row, "sequence_number", 0) or 0),
                _text(_get(row, "productFilename")),
                _text(_get(row, "dataURI")),
            ),
        )
        if not products:
            return self._result("no_products", target, sectors, flux_kind, radius_arcsec, [], [], 0, retrieved_at)

        by_group: dict[str, Any] = {}
        for row in observations:
            group_id = _get(row, "obsid")
            if group_id is not None:
                by_group[_text(group_id)] = row

        canonical: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        cache_hits = downloads = 0
        for product in products:
            data_uri = _text(_get(product, "dataURI"))
            product_filename = _text(_get(product, "productFilename")) or data_uri.rsplit("/", 1)[-1]
            parent_group = _text(_get(product, "parent_obsid") or _get(product, "obsID"))
            observation_row = by_group.get(parent_group)
            sequence = _get(product, "sequence_number") or (_get(observation_row, "sequence_number") if observation_row else None)
            metadata = {
                "archive": "MAST",
                "data_uri": data_uri,
                "product_filename": product_filename,
                "product_version": _get(product, "prvversion"),
                "mast_obs_id": _get(observation_row, "obs_id") if observation_row is not None else _get(product, "obs_id"),
                "mast_obsid": parent_group or _get(product, "obsid"),
                "sequence_number": int(sequence) if sequence is not None else None,
                "retrieved_at": retrieved_at,
                "product_url": _mast_source_url(data_uri),
            }
            failure_product = {"sequence_number": metadata["sequence_number"], "data_uri": data_uri}
            try:
                if not data_uri:
                    raise ValueError("MAST product has no data URI")
                local_path, cache_hit, cache_relative = self._download_product(
                    data_uri, product_filename, metadata["product_version"]
                )
                metadata["cache_path"] = cache_relative
                result = read_tess_light_curve(
                    local_path,
                    flux_kind=flux_kind,
                    source_url=metadata["product_url"],
                    product_metadata=metadata,
                )
                actual_sector = result["provenance"].get("sector")
                if (
                    metadata["sequence_number"] is not None
                    and actual_sector is not None
                    and int(actual_sector) != metadata["sequence_number"]
                ):
                    raise ValueError(
                        f"product sector {actual_sector} does not match MAST sector {metadata['sequence_number']}"
                    )
                actual_tic = _canonical_tic(result["provenance"].get("tic_id"))
                if tic_id and actual_tic and actual_tic != tic_id:
                    raise ValueError(f"FITS TIC {actual_tic} does not match requested TIC {tic_id}")
                if target_ra is not None and target_dec is not None:
                    separation = angular_separation_arcsec(target_ra, target_dec, result["ra_deg"], result["dec_deg"])
                    if separation > radius_arcsec:
                        raise ValueError(
                            f"FITS target is {separation:.3f} arcsec from the requested coordinates "
                            f"(limit {radius_arcsec:.3f} arcsec)"
                        )
                result["provenance"]["cache_hit"] = cache_hit
                canonical.append(result)
                cache_hits += int(cache_hit)
                downloads += int(not cache_hit)
            except Exception as exc:
                category = "target_mismatch" if isinstance(exc, ValueError) and (
                    "does not match" in str(exc) or "from the requested coordinates" in str(exc)
                ) else "product_unavailable"
                failures.append(self._failure(failure_product, category, str(exc)))
                logger.warning("tess_product_failed", data_uri=data_uri, error=str(exc))

        if canonical:
            state = "partial" if failures else "complete"
        elif failures and all(failure["error_type"] == "target_mismatch" for failure in failures):
            state = "no_products"
        else:
            state = "failed" if failures else "no_products"
        return self._result(
            state, target, sectors, flux_kind, radius_arcsec, canonical, failures,
            len(products), retrieved_at, cache_hits=cache_hits, downloads=downloads,
        )

    @staticmethod
    def _result(
        state: str,
        target: dict[str, Any],
        sectors: list[int] | None,
        flux_kind: str,
        radius_arcsec: float,
        observations: list[dict[str, Any]],
        failures: list[dict[str, Any]],
        product_count: int,
        retrieved_at: str,
        *,
        cache_hits: int = 0,
        downloads: int = 0,
    ) -> dict[str, Any]:
        return {
            "status": state,
            "target": dict(target),
            "retrieved_at": retrieved_at,
            "selection": {
                "sectors": sectors,
                "flux_kind": flux_kind,
                "radius_arcsec": radius_arcsec,
                "pipeline": "SPOC",
            },
            "product_count": product_count,
            "observation_count": len(observations),
            "cache": {"cache_hits": cache_hits, "downloads": downloads},
            "observations": observations,
            "failures": failures,
        }

# ===== TESS command-line interface =====
def _positive_sector(value: str) -> int:
    sector = int(value)
    if sector <= 0:
        raise argparse.ArgumentTypeError("sector must be positive")
    return sector


def _add_plot_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--normalize", action="store_true", help="Display relative flux normalized to median")
    parser.add_argument("--show-uncertainties", action="store_true", help="Render FITS flux uncertainties")
    parser.add_argument("--quality-display", choices=["highlight", "hide"], default="highlight",
                        help="Highlight quality-flagged points or hide them from the figure")
    parser.add_argument("--period-days", type=float, help="Fold the display on this caller-supplied period")
    parser.add_argument("--epoch-btjd", type=float, help="Phase-zero epoch in BTJD; requires --period-days")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="astrosearch-tess", description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    retrieve = commands.add_parser("retrieve", help="Search MAST and convert TESS SPOC light curves")
    target = retrieve.add_mutually_exclusive_group()
    target.add_argument("--tic-id", help="TIC identifier (digits, optionally prefixed by TIC)")
    target.add_argument("--name", help="Object name resolved using the configured CDS Sesame endpoint")
    target.add_argument("--ra", type=float, help="Right ascension in degrees; use with --dec")
    retrieve.add_argument("--dec", type=float, help="Declination in degrees; use with --ra")
    retrieve.add_argument("--sector", dest="sectors", type=_positive_sector, action="append",
                          help="TESS sector to retrieve; repeat to select multiple (default: all)")
    retrieve.add_argument("--radius-arcsec", type=float, default=5.0,
                          help="Coordinate query and target verification radius (default: 5 arcsec)")
    retrieve.add_argument("--flux-kind", choices=["SAP_FLUX", "PDCSAP_FLUX"], default="PDCSAP_FLUX")
    retrieve.add_argument("--cache-dir", type=Path, help="Persistent FITS cache (or TESS_CACHE_DIR)")
    retrieve.add_argument("--jsonl", type=Path, help="Write successful canonical observations, one per line")
    retrieve.add_argument("--plot-dir", type=Path, help="Also write one PNG per retrieved product")
    _add_plot_options(retrieve)

    plot = commands.add_parser("plot", help="Plot one or more local TESS SPOC light-curve FITS files")
    plot.add_argument("fits", nargs="+", type=Path, help="Local SPOC light-curve FITS product(s)")
    plot.add_argument("--output-dir", type=Path, required=True, help="Directory for one PNG per FITS product")
    plot.add_argument("--flux-kind", choices=["SAP_FLUX", "PDCSAP_FLUX"], default="PDCSAP_FLUX")
    _add_plot_options(plot)
    return parser


def _validate_plot_options(parser: argparse.ArgumentParser, arguments: argparse.Namespace) -> None:
    if arguments.epoch_btjd is not None and arguments.period_days is None:
        parser.error("--epoch-btjd requires --period-days")
    if arguments.period_days is not None and (not math.isfinite(arguments.period_days) or arguments.period_days <= 0):
        parser.error("--period-days must be positive and finite")
    if arguments.epoch_btjd is not None and not math.isfinite(arguments.epoch_btjd):
        parser.error("--epoch-btjd must be finite")


def _resolve_target(arguments: argparse.Namespace) -> dict[str, Any]:
    if arguments.tic_id:
        tic_id = re.sub(r"^TIC\s*", "", arguments.tic_id.strip(), flags=re.IGNORECASE)
        if not tic_id.isdigit():
            raise ValueError("--tic-id must contain a numeric TIC identifier")
        return {"tic_id": tic_id}
    if arguments.name:
        settings = Settings()

        async def resolve() -> dict[str, Any]:
            async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, follow_redirects=True) as client:
                result = await SesameResolver(client, endpoint=settings.resolver_endpoint).resolve(arguments.name)
            return {
                "name": arguments.name,
                "ra_deg": result.ra_deg,
                "dec_deg": result.dec_deg,
                "resolved_object": result.as_dict(),
            }

        return asyncio.run(resolve())
    if arguments.ra is not None and arguments.dec is not None:
        if not 0 <= arguments.ra < 360 or not -90 <= arguments.dec <= 90:
            raise ValueError("coordinates must satisfy 0 <= RA < 360 and -90 <= Dec <= 90")
        return {"ra_deg": arguments.ra, "dec_deg": arguments.dec}
    raise ValueError("provide exactly one target: --tic-id, --name, or both --ra and --dec")


def _write_jsonl(path: Path, observations: list[dict[str, Any]]) -> None:
    if not observations:
        return
    path = path.expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            for observation in observations:
                handle.write(json.dumps(observation, ensure_ascii=False, allow_nan=False, separators=(",", ":")))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _save_plots(
    observations: list[dict[str, Any]],
    output_dir: Path,
    *,
    normalize: bool,
    show_uncertainties: bool,
    quality_display: str,
    period_days: float | None,
    epoch_btjd: float | None,
) -> list[Path]:
    output_dir = output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for index, observation in enumerate(observations, start=1):
        identity = observation.get("observation_id") or f"light-curve-{index}"
        filename = re.sub(r"[^A-Za-z0-9._-]+", "_", str(identity)).strip("._") or f"light-curve-{index}"
        path = output_dir / f"{filename}.png"
        data = plot_light_curves(
            [observation],
            show_uncertainties=show_uncertainties,
            quality_display=quality_display,
            normalize=normalize,
            period_days=period_days,
            epoch_btjd=epoch_btjd,
        )
        path.write_bytes(data)
        written.append(path)
    return written


def _retrieve(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    if (arguments.ra is None) != (arguments.dec is None):
        parser.error("--ra and --dec must be supplied together")
    if not math_is_finite_positive(arguments.radius_arcsec):
        parser.error("--radius-arcsec must be positive and finite")
    _validate_plot_options(parser, arguments)
    target = _resolve_target(arguments)
    service = TessLightCurveService(arguments.cache_dir)
    result = service.retrieve(
        target,
        sectors=arguments.sectors,
        flux_kind=arguments.flux_kind,
        radius_arcsec=arguments.radius_arcsec,
    )
    if result["observations"]:
        if arguments.jsonl:
            _write_jsonl(arguments.jsonl, result["observations"])
        if arguments.plot_dir:
            result["plots"] = [str(path) for path in _save_plots(
                result["observations"], arguments.plot_dir,
                normalize=arguments.normalize,
                show_uncertainties=arguments.show_uncertainties,
                quality_display=arguments.quality_display,
                period_days=arguments.period_days,
                epoch_btjd=arguments.epoch_btjd,
            )]
    print(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False))
    for failure in result["failures"]:
        print(f"Product failure ({failure['error_type']}): {failure['product_id']}: {failure['message']}", file=sys.stderr)
    if result["status"] == "no_products":
        print("No TESS SPOC light-curve products matched this target and selection.", file=sys.stderr)
        return 0
    return 2 if result["status"] == "failed" else 0


def math_is_finite_positive(value: float) -> bool:
    return math.isfinite(value) and value > 0


def _plot_local(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    _validate_plot_options(parser, arguments)
    observations = []
    for path in arguments.fits:
        if not path.is_file():
            parser.error(f"FITS file not found: {path}")
        observation = read_tess_light_curve(path, flux_kind=arguments.flux_kind)
        path_key = hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()[:10]
        observation["observation_id"] = f"{observation['observation_id']}-local-{path_key}"
        observation["title"] = f"{observation['title']} — {path.name}"
        observations.append(observation)
    paths = _save_plots(
        observations, arguments.output_dir,
        normalize=arguments.normalize,
        show_uncertainties=arguments.show_uncertainties,
        quality_display=arguments.quality_display,
        period_days=arguments.period_days,
        epoch_btjd=arguments.epoch_btjd,
    )
    print(json.dumps({"observation_count": len(observations), "plots": [str(path) for path in paths]}, indent=2))
    return 0


def main() -> None:
    parser = _parser()
    arguments = parser.parse_args()
    try:
        code = _retrieve(arguments, parser) if arguments.command == "retrieve" else _plot_local(arguments, parser)
    except (TessServiceError, ObjectResolutionError, ValueError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    raise SystemExit(code)

if __name__ == "__main__":
    main()
