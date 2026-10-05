"""Signal representations, cross-reference, immutable delivery ingestion, and plots."""



from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import math
import shutil
import uuid

import numpy as np

from astronomy import digest, load_json, state_lock, utcnow, write_json
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

# ===== signal representations =====
REPRESENTATION_VERSION = "astrosearch-signal-v1"
VECTOR_SIZE = 48
MIN_SAMPLES = 8


class RepresentationError(ValueError):
    """The observation cannot be represented without inventing data."""


@dataclass(frozen=True)
class SignalRepresentation:
    observation_id: str
    object_id: str | None
    modality: Literal["light_curve", "spectrum"]
    vector: list[float]
    sample_count: int
    input_sample_count: int
    good_fraction: float
    axis_min: float
    axis_max: float
    value_median: float
    value_scale: float
    version: str = REPRESENTATION_VERSION

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def _finite_number(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise RepresentationError(f"{name} must be numeric") from exc
    if not math.isfinite(number):
        raise RepresentationError(f"{name} must be finite")
    return number


def _prepare(observation: dict[str, Any]) -> tuple[np.ndarray, np.ndarray, np.ndarray | None, int]:
    try:
        axis = np.asarray(observation.get("axis", []), dtype=float)
        values = np.asarray(observation.get("values", []), dtype=float)
    except (TypeError, ValueError) as exc:
        raise RepresentationError("axis and values must contain numeric samples") from exc
    if axis.ndim != 1 or values.ndim != 1 or len(axis) != len(values):
        raise RepresentationError("axis and values must be one-dimensional arrays of equal length")
    input_count = len(axis)
    if input_count < MIN_SAMPLES:
        raise RepresentationError(f"at least {MIN_SAMPLES} samples are required")
    mask = np.isfinite(axis) & np.isfinite(values)
    quality = observation.get("quality")
    if quality is not None:
        try:
            flags = np.asarray(quality, dtype=float)
        except (TypeError, ValueError) as exc:
            raise RepresentationError("quality must contain numeric flags") from exc
        if flags.ndim != 1 or len(flags) != input_count:
            raise RepresentationError("quality must have one entry per sample")
        mask &= flags == 0
    errors = observation.get("uncertainties")
    clean_errors = None
    if errors is not None:
        try:
            clean_errors = np.asarray(errors, dtype=float)
        except (TypeError, ValueError) as exc:
            raise RepresentationError("uncertainties must contain numeric samples") from exc
        if clean_errors.ndim != 1 or len(clean_errors) != input_count:
            raise RepresentationError("uncertainties must have one entry per sample")
        mask &= np.isfinite(clean_errors) & (clean_errors > 0)
    axis, values = axis[mask], values[mask]
    clean_errors = clean_errors[mask] if clean_errors is not None else None
    if len(axis) < MIN_SAMPLES:
        raise RepresentationError(f"fewer than {MIN_SAMPLES} usable samples remain after quality filtering")
    order = np.argsort(axis, kind="stable")
    axis, values = axis[order], values[order]
    clean_errors = clean_errors[order] if clean_errors is not None else None
    if axis[-1] <= axis[0]:
        raise RepresentationError("axis must span more than one coordinate")
    return axis, values, clean_errors, input_count


def _shape_bins(axis: np.ndarray, values: np.ndarray, count: int = 32) -> np.ndarray:
    scaled = (axis - axis[0]) / (axis[-1] - axis[0])
    edges = np.linspace(0.0, 1.0, count + 1)
    centers = (edges[:-1] + edges[1:]) / 2
    result = np.empty(count, dtype=float)
    for index in range(count):
        upper = scaled <= edges[index + 1] if index == count - 1 else scaled < edges[index + 1]
        selected = values[(scaled >= edges[index]) & upper]
        result[index] = np.median(selected) if len(selected) else np.nan
    missing = np.isnan(result)
    result[missing] = np.interp(centers[missing], centers[~missing], result[~missing])
    return result


def represent_signal(observation: dict[str, Any]) -> SignalRepresentation:
    """Convert a light curve or spectrum to a fixed, interpretable vector."""
    modality = observation.get("modality")
    if modality not in {"light_curve", "spectrum"}:
        raise RepresentationError("modality must be light_curve or spectrum")
    observation_id = str(observation.get("observation_id") or "").strip()
    if not observation_id:
        raise RepresentationError("observation_id is required")
    axis, values, errors, input_count = _prepare(observation)
    physical_axis_min, physical_axis_max = float(axis[0]), float(axis[-1])
    if modality == "spectrum" and np.all(axis > 0):
        axis = np.log(axis)
    median = float(np.median(values))
    mad = float(np.median(np.abs(values - median)))
    scale = 1.4826 * mad
    if not math.isfinite(scale) or scale <= 0:
        scale = float(np.std(values))
    if not math.isfinite(scale) or scale <= 0:
        raise RepresentationError("signal has no measurable variation")
    normalized = (values - median) / scale
    weights = None if errors is None else 1.0 / np.square(errors / scale)
    x = (axis - axis.mean()) / (axis[-1] - axis[0])
    slope = float(np.polyfit(x, normalized, 1, w=None if weights is None else np.sqrt(weights))[0])
    centered = normalized - normalized.mean()
    std = float(np.std(normalized))
    skew = float(np.mean(centered ** 3) / std ** 3) if std else 0.0
    kurtosis = float(np.mean(centered ** 4) / std ** 4 - 3) if std else 0.0
    derivative = np.diff(normalized) / np.maximum(np.diff(axis), np.finfo(float).eps)
    stats = np.asarray([
        float(np.mean(normalized)), std, float(np.min(normalized)), float(np.max(normalized)),
        *[float(v) for v in np.quantile(normalized, [0.05, 0.25, 0.75, 0.95])],
        skew, kurtosis, float(np.median(np.abs(derivative))), slope,
    ])
    correlations = []
    for lag in (1, 2, 4, 8):
        if len(normalized) <= lag or np.std(normalized[:-lag]) == 0 or np.std(normalized[lag:]) == 0:
            correlations.append(0.0)
        else:
            correlations.append(float(np.corrcoef(normalized[:-lag], normalized[lag:])[0, 1]))
    vector = np.nan_to_num(np.concatenate([stats, _shape_bins(axis, normalized), correlations]))
    norm = float(np.linalg.norm(vector))
    if norm:
        vector /= norm
    return SignalRepresentation(
        observation_id=observation_id,
        object_id=str(observation["object_id"]).strip() if observation.get("object_id") else None,
        modality=modality,
        vector=[float(v) for v in vector],
        sample_count=len(axis), input_sample_count=input_count, good_fraction=len(axis) / input_count,
        axis_min=physical_axis_min, axis_max=physical_axis_max, value_median=median, value_scale=scale,
    )


def angular_separation_arcsec(ra1: float, dec1: float, ra2: float, dec2: float) -> float:
    a1, d1, a2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
    haversine = math.sin((d2 - d1) / 2) ** 2 + math.cos(d1) * math.cos(d2) * math.sin((a2 - a1) / 2) ** 2
    return math.degrees(2 * math.asin(min(1.0, math.sqrt(haversine)))) * 3600


def cosine_distance(left: list[float], right: list[float]) -> float:
    try:
        a, b = np.asarray(left, dtype=float), np.asarray(right, dtype=float)
    except (TypeError, ValueError) as exc:
        raise RepresentationError("reference vectors must contain numeric values") from exc
    if a.ndim != 1 or b.ndim != 1 or len(a) != len(b) or not len(a):
        raise RepresentationError("reference vectors must be one-dimensional and match the observation vector length")
    if not np.isfinite(a).all() or not np.isfinite(b).all() or np.linalg.norm(a) == 0 or np.linalg.norm(b) == 0:
        raise RepresentationError("reference vectors must contain finite, non-zero values")
    return float(np.clip(1 - np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)), 0, 2))


def cross_reference_observation(observation: dict[str, Any], references: list[dict[str, Any]], *,
                                radius_arcsec: float = 2.0, representation_threshold: float = 0.25,
                                anomaly_threshold: float = 0.45) -> dict[str, Any]:
    """Rank known counterparts and gate an observation for human novelty review."""
    try:
        radius_arcsec, representation_threshold, anomaly_threshold = map(
            float, (radius_arcsec, representation_threshold, anomaly_threshold)
        )
    except (TypeError, ValueError) as exc:
        raise RepresentationError("cross-reference thresholds must be numeric") from exc
    if (
        not all(math.isfinite(value) for value in (radius_arcsec, representation_threshold, anomaly_threshold))
        or radius_arcsec <= 0
        or not 0 <= representation_threshold <= 2
        or not 0 <= anomaly_threshold <= 2
    ):
        raise RepresentationError("invalid cross-reference threshold")
    representation = represent_signal(observation)
    ra, dec = _finite_number(observation.get("ra_deg"), "ra_deg"), _finite_number(observation.get("dec_deg"), "dec_deg")
    if not 0 <= ra < 360 or not -90 <= dec <= 90:
        raise RepresentationError("coordinates must satisfy 0 <= RA < 360 and -90 <= Dec <= 90")
    candidates, nearest = [], None
    for reference in references:
        separation = angular_separation_arcsec(ra, dec, _finite_number(reference.get("ra_deg"), "reference ra_deg"),
                                               _finite_number(reference.get("dec_deg"), "reference dec_deg"))
        distance = None
        compatible = (reference.get("modality") == representation.modality and
                      reference.get("representation_version") == representation.version)
        if reference.get("representation") is not None and compatible:
            distance = cosine_distance(representation.vector, reference["representation"])
            nearest = distance if nearest is None else min(nearest, distance)
        if separation <= radius_arcsec:
            candidates.append({"catalog_id": str(reference.get("catalog_id") or ""), "title": reference.get("title"),
                               "separation_arcsec": separation, "representation_distance": distance,
                               "position_match": True,
                               "representation_compatible": compatible,
                               "representation_match": distance is not None and distance <= representation_threshold})
    candidates.sort(key=lambda item: (item["separation_arcsec"], item["representation_distance"] or 3.0))
    supported = [candidate for candidate in candidates if candidate["representation_match"]]
    if len(supported) == 1:
        status = "matched"
    elif len(supported) > 1:
        status = "ambiguous"
    elif candidates:
        status = "position_only_review"
    elif nearest is None:
        status = "insufficient_reference_coverage"
    elif nearest >= anomaly_threshold and representation.good_fraction >= 0.8:
        status = "novelty_review_candidate"
    else:
        status = "unmatched"
    result = {
        "schema_version": 1, "status": status, "discovery_claim": False,
        "representation": representation.as_dict(), "coordinates": {"frame": "ICRS", "ra_deg": ra, "dec_deg": dec},
        "thresholds": {"radius_arcsec": radius_arcsec, "representation_distance": representation_threshold,
                       "anomaly_distance": anomaly_threshold},
        "nearest_representation_distance": nearest, "candidates": candidates,
        "required_follow_up": [
            "inspect pixels or detector-level data for artifacts and blending",
            "cross-match at the observation epoch with proper motion and solar-system ephemerides",
            "seek independent repeat observations or another instrument",
            "obtain domain-expert review before describing the source as a discovery",
        ],
    }
    result["fingerprint"] = hashlib.sha256(json.dumps(result, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return result


def mantis_signal_row(observation: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
    rep = result["representation"]
    return {
        "catalog_id": "observation:" + rep["observation_id"], "title": observation.get("title") or rep["observation_id"],
        "kind": "astronomical_signal", "object_id": rep["object_id"] or "", "modality": rep["modality"],
        "instrument": observation.get("instrument") or "", "band": observation.get("band") or "",
        "triage_status": result["status"],
        "summary": f"{rep['modality']} observation with {rep['sample_count']} usable samples; triage status {result['status']}.",
        "ra_deg": result["coordinates"]["ra_deg"], "dec_deg": result["coordinates"]["dec_deg"],
        "sample_count": rep["sample_count"], "good_fraction": rep["good_fraction"],
        "nearest_representation_distance": result["nearest_representation_distance"],
        "representation": json.dumps(rep["vector"], separators=(",", ":")), "representation_version": rep["version"],
        "observed_at": observation.get("observed_at") or "", "source_url": observation.get("source_url") or "",
        "provenance": json.dumps(observation.get("provenance") or {}, sort_keys=True, separators=(",", ":")),
    }

# ===== delivery ingestion and Mantis export =====
SIGNAL_FIELDS = [
    "catalog_id", "title", "kind", "object_id", "modality", "instrument", "band", "triage_status", "summary",
    "ra_deg", "dec_deg", "sample_count", "good_fraction", "nearest_representation_distance", "representation",
    "representation_version", "observed_at", "source_url", "provenance",
]


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    records = []
    with path.open(encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number} of {path}") from exc
            if not isinstance(value, dict):
                raise TypeError(f"line {line_number} of {path} is not an object")
            records.append(value)
    return records


def _commit_signal_batch(root: Path, observations: list[dict[str, Any]], references: list[dict[str, Any]], *,
                         radius_arcsec: float = 2.0, representation_threshold: float = 0.25,
                         anomaly_threshold: float = 0.45) -> dict[str, Any]:
    """Validate, deduplicate, represent, triage, and atomically commit a delivery."""
    if not observations:
        raise ValueError("refusing to commit an empty signal delivery")
    by_id: dict[str, dict[str, Any]] = {}
    for observation in observations:
        observation_id = str(observation.get("observation_id") or "").strip()
        if not observation_id:
            raise ValueError("every observation requires observation_id")
        if observation_id in by_id and digest(by_id[observation_id]) != digest(observation):
            raise ValueError(f"conflicting duplicate observation_id: {observation_id}")
        by_id[observation_id] = observation
    ordered = [by_id[key] for key in sorted(by_id)]
    results = [cross_reference_observation(
        observation, references, radius_arcsec=radius_arcsec,
        representation_threshold=representation_threshold, anomaly_threshold=anomaly_threshold,
    ) for observation in ordered]
    payload = {
        "schema_version": 1, "representation_version": results[0]["representation"]["version"],
        "observations": ordered, "reference_fingerprint": digest(references),
        "thresholds": {"radius_arcsec": radius_arcsec, "representation_distance": representation_threshold,
                       "anomaly_distance": anomaly_threshold},
    }
    fingerprint = digest(payload)
    previous = load_json(root / "latest-signals.json")
    if previous and previous["fingerprint"] == fingerprint:
        write_json(root / "last-signal-check.json", {"checked_at": utcnow(), "changed": False, "fingerprint": fingerprint})
        return {**previous, "changed": False}
    rows = [mantis_signal_row(observation, result) for observation, result in zip(ordered, results, strict=True)]
    final = root / "signal-snapshots" / fingerprint
    stage = root / "signal-snapshots" / (".staging-" + uuid.uuid4().hex)
    manifest = {
        "schema_version": 1, "fingerprint": fingerprint, "created_at": utcnow(), "observation_count": len(ordered),
        "reference_count": len(references), "status_counts": dict(Counter(result["status"] for result in results)),
        "representation_version": results[0]["representation"]["version"],
    }
    try:
        stage.mkdir(parents=True)
        write_json(stage / "delivery.json", payload)
        write_json(stage / "cross-reference-results.json", results)
        write_json(stage / "manifest.json", manifest)
        with (stage / "mantis-signals.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=SIGNAL_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        if final.exists():
            shutil.rmtree(stage)
        else:
            stage.rename(final)
        write_json(root / "latest-signals.json", manifest)
        write_json(root / "last-signal-check.json", {"checked_at": utcnow(), "changed": True, "fingerprint": fingerprint})
    except BaseException:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return {**manifest, "changed": True, "snapshot_path": str(final)}


def commit_signal_batch(root: Path, observations: list[dict[str, Any]], references: list[dict[str, Any]], *,
                        radius_arcsec: float = 2.0, representation_threshold: float = 0.25,
                        anomaly_threshold: float = 0.45) -> dict[str, Any]:
    """Commit one delivery while excluding concurrent catalog or signal writers."""
    with state_lock(root):
        return _commit_signal_batch(
            root, observations, references, radius_arcsec=radius_arcsec,
            representation_threshold=representation_threshold, anomaly_threshold=anomaly_threshold,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("observations", type=Path, help="JSON Lines telescope delivery")
    parser.add_argument("--references", type=Path, required=True, help="JSON array of catalog/reference objects")
    parser.add_argument("--state-dir", type=Path, default=Path("astronomy-data"))
    parser.add_argument("--radius-arcsec", type=float, default=2.0)
    parser.add_argument("--representation-threshold", type=float, default=0.25)
    parser.add_argument("--anomaly-threshold", type=float, default=0.45)
    arguments = parser.parse_args()
    references = json.loads(arguments.references.read_text(encoding="utf-8"))
    if not isinstance(references, list):
        raise SystemExit("references must be a JSON array")
    result = commit_signal_batch(
        arguments.state_dir.resolve(), load_jsonl(arguments.observations), references,
        radius_arcsec=arguments.radius_arcsec, representation_threshold=arguments.representation_threshold,
        anomaly_threshold=arguments.anomaly_threshold,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))

# ===== signal plotting =====
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt


QualityDisplay = Literal["highlight", "hide"]


def _display_arrays(observation: dict[str, Any], *, normalize: bool) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    axis = np.asarray(observation.get("axis", []), dtype=float)
    values = np.asarray(observation.get("values", []), dtype=float)
    if axis.ndim != 1 or values.ndim != 1 or len(axis) != len(values):
        raise ValueError("observation axis and values must be one-dimensional arrays of equal length")
    uncertainties_data = observation.get("uncertainties")
    if uncertainties_data is None:
        uncertainties = np.full(len(values), np.nan)
    else:
        uncertainties = np.asarray(uncertainties_data, dtype=float)
        if uncertainties.ndim != 1 or len(uncertainties) != len(values):
            raise ValueError("uncertainties must have one entry per signal sample")
    quality_data = observation.get("quality")
    if quality_data is None:
        flagged = np.zeros(len(values), dtype=bool)
    else:
        quality = np.asarray(quality_data)
        if quality.ndim != 1 or len(quality) != len(values):
            raise ValueError("quality must have one entry per signal sample")
        flagged = quality != 0
    valid = np.isfinite(axis) & np.isfinite(values)
    axis, values, uncertainties, flagged = axis[valid], values[valid], uncertainties[valid], flagged[valid]
    if normalize and len(values):
        center = float(np.nanmedian(values))
        if not math.isfinite(center) or center == 0:
            raise ValueError("cannot normalize a light curve with a non-finite or zero median")
        values = values / center
        uncertainties = uncertainties / abs(center)
    return axis, values, uncertainties, flagged


def plot_light_curves(
    observations: list[dict[str, Any]],
    *,
    show_uncertainties: bool = False,
    quality_display: QualityDisplay = "highlight",
    normalize: bool = False,
    period_days: float | None = None,
    epoch_btjd: float | None = None,
    title: str | None = None,
) -> bytes:
    """Render one or more canonical observations to a PNG image.

    All transformations are local to the rendered arrays. The caller's raw
    samples and quality flags are never modified.
    """
    if not observations:
        raise ValueError("at least one observation is required for a plot")
    if quality_display not in {"highlight", "hide"}:
        raise ValueError("quality_display must be highlight or hide")
    if period_days is not None and (not math.isfinite(period_days) or period_days <= 0):
        raise ValueError("period_days must be a positive finite number")
    if epoch_btjd is not None and not math.isfinite(epoch_btjd):
        raise ValueError("epoch_btjd must be finite")

    rows = len(observations)
    figure, axes = plt.subplots(rows, 1, figsize=(11, max(3.2, 3.2 * rows)), squeeze=False, constrained_layout=True)
    try:
        for index, (observation, axis_plot) in enumerate(zip(observations, axes[:, 0], strict=True)):
            axis, values, uncertainties, flagged = _display_arrays(observation, normalize=normalize)
            if period_days is not None:
                reference = epoch_btjd if epoch_btjd is not None else (float(np.min(axis)) if len(axis) else 0.0)
                axis = ((axis - reference) / period_days) % 1.0
                axis = np.where(axis > 0.5, axis - 1.0, axis)
                order = np.argsort(axis, kind="stable")
                axis, values, uncertainties, flagged = (
                    axis[order], values[order], uncertainties[order], flagged[order]
                )

            visible = np.ones(len(values), dtype=bool) if quality_display == "highlight" else ~flagged
            good = visible & ~flagged
            bad = visible & flagged
            if show_uncertainties and np.any(np.isfinite(uncertainties[good]) & (uncertainties[good] >= 0)):
                error_mask = good & np.isfinite(uncertainties) & (uncertainties >= 0)
                axis_plot.errorbar(
                    axis[error_mask], values[error_mask], yerr=uncertainties[error_mask],
                    fmt=".", ms=3, lw=0.6, capsize=0, color="#2767a5", alpha=0.8, label="QUALITY = 0",
                )
                no_error = good & ~error_mask
                if np.any(no_error):
                    axis_plot.plot(axis[no_error], values[no_error], ".", ms=3, color="#2767a5", label="QUALITY = 0")
            elif np.any(good):
                axis_plot.plot(axis[good], values[good], ".", ms=3, color="#2767a5", label="QUALITY = 0")
            if np.any(bad):
                axis_plot.scatter(
                    axis[bad], values[bad], marker="x", s=22, linewidths=0.9,
                    color="#c23b33", alpha=0.9, label="QUALITY != 0", zorder=3,
                )

            provenance = observation.get("provenance") or {}
            sector = provenance.get("sector", "unknown")
            flux_kind = provenance.get("value_kind", "flux")
            flux_unit = provenance.get("value_unit") or ""
            plot_label = str(
                observation.get("title")
                or observation.get("object_id")
                or observation.get("observation_id")
                or f"Observation {index + 1}"
            )
            axis_plot.set_title(f"{plot_label} — sector {sector} — {flux_kind}")
            if period_days is None:
                axis_format = provenance.get("time_format") or "BTJD"
                axis_scale = provenance.get("time_scale") or "TDB"
                axis_plot.set_xlabel(f"Time ({axis_format}, {axis_scale})")
            else:
                axis_plot.set_xlabel("Phase (cycles, centered on zero)")
            value_label = f"{flux_kind} ({flux_unit})" if flux_unit else str(flux_kind)
            axis_plot.set_ylabel("Relative flux (median = 1)" if normalize else value_label)
            axis_plot.grid(True, alpha=0.2)
            if np.any(good) or np.any(bad):
                axis_plot.legend(loc="best", fontsize="small")
        if title:
            figure.suptitle(title)
        output = io.BytesIO()
        figure.savefig(output, format="png", dpi=150, metadata={"Software": "AstroSearch"})
        return output.getvalue()
    finally:
        plt.close(figure)

if __name__ == "__main__":
    main()
