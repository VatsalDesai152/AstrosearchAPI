"""Astronomy summaries, archive snapshots, publication, and catalog plotting workflows."""



from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
import uuid
import xml.etree.ElementTree as ET

import httpx
import numpy as np
from astropy.io.votable import parse_single_table
from collections import Counter, defaultdict
from contextlib import contextmanager
from core import CatalogRegistry
from datetime import UTC, datetime
from main import crossmatch, search_object
from pathlib import Path
from typing import Any
from urllib.parse import quote

# ===== evidence summaries =====
NEA_TAP = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
SIMBAD_TAP = "https://simbad.cds.unistra.fr/simbad/sim-tap/sync"
NEA_DOCS = "https://exoplanetarchive.ipac.caltech.edu/docs/API_PS_columns.html"
SCHEMA_VERSION = 2
MEASURES = {
    "pl_orbper": ("orbital period", "days"),
    "pl_orbsmax": ("semi-major axis", "AU"),
    "pl_rade": ("radius", "Earth radii"),
    "pl_bmasse": ("reported mass parameter", "Earth masses"),
    "pl_orbeccen": ("orbital eccentricity", ""),
    "pl_eqt": ("equilibrium temperature", "K"),
    "st_teff": ("host effective temperature", "K"),
    "st_mass": ("host mass", "solar masses"),
    "st_rad": ("host radius", "solar radii"),
    "sy_dist": ("system distance", "pc"),
}
ALIAS_COLUMNS = ("hostname", "gaia_dr3_id", "gaia_dr2_id", "hd_name", "hip_name", "tic_id")
BASE_COLUMNS = [
    "pl_name", *ALIAS_COLUMNS, "ra", "dec", "sy_snum", "sy_pnum", "cb_flag",
    "discoverymethod", "disc_year", "disc_facility", "disc_refname", "pl_controv_flag",
    "pl_bmassprov", "rowupdate",
]


def utcnow() -> str:
    return datetime.now(UTC).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def entity_id(kind: str, name: str) -> str:
    # Preserve case: binary star A and planet a must never silently collapse.
    return f"{kind}:{quote(name.strip(), safe='')}"


def literal(value: str) -> str:
    if not value or len(value) > 300 or any(ord(c) < 32 for c in value):
        raise ValueError("Object names must contain 1–300 printable characters")
    return "'" + value.replace("'", "''") + "'"


class ArchiveError(RuntimeError):
    """An upstream response is unavailable, invalid, or incomplete."""


def parse_tap(content: bytes) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(content)
        statuses = [e.attrib.get("value", "").upper() for e in root.iter()
                    if e.tag.split("}")[-1] == "INFO" and e.attrib.get("name") == "QUERY_STATUS"]
        if not statuses or any(s != "OK" for s in statuses):
            raise ArchiveError(f"TAP result rejected: QUERY_STATUS={statuses}")
        table = parse_single_table(io.BytesIO(content)).to_table()
        rows = []
        for row in table:
            clean = {}
            for key in table.colnames:
                value = row[key]
                if np.ma.is_masked(value):
                    value = None
                elif isinstance(value, np.generic):
                    value = value.item()
                if isinstance(value, bytes):
                    value = value.decode("utf-8")
                if isinstance(value, float) and not math.isfinite(value):
                    value = None
                clean[key] = value
            rows.append(clean)
        return rows
    except ArchiveError:
        raise
    except Exception as exc:
        raise ArchiveError("Invalid TAP VOTable response") from exc


class TapClient:
    def __init__(self, client: httpx.AsyncClient, *, delay: float = 0.2):
        self.client = client
        self.delay = delay
        self.provenance: list[dict[str, Any]] = []

    async def query(self, endpoint: str, query: str, *, maxrec: int = 20000) -> list[dict[str, Any]]:
        for attempt in range(4):
            try:
                async with self.client.stream("POST", endpoint, data={
                    "REQUEST": "doQuery", "LANG": "ADQL", "QUERY": query,
                    "FORMAT": "votable/td" if endpoint == SIMBAD_TAP else "votable", "MAXREC": str(maxrec),
                }, timeout=120) as response:
                    response.raise_for_status()
                    data = bytearray()
                    async for chunk in response.aiter_bytes():
                        data.extend(chunk)
                        if len(data) > 64 * 1024 * 1024:
                            raise ArchiveError("TAP response exceeded 64 MiB safety limit")
                rows = parse_tap(bytes(data))
                self.provenance.append({"endpoint": endpoint, "query": query, "retrieved_at": utcnow(),
                                        "rows": len(rows), "response_sha256": hashlib.sha256(data).hexdigest()})
                if self.delay:
                    await asyncio.sleep(self.delay)
                return rows
            except (httpx.TransportError, httpx.HTTPStatusError) as exc:
                retryable = not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code in (429, 500, 502, 503, 504)
                if not retryable or attempt == 3:
                    raise ArchiveError(f"TAP request failed at {endpoint}") from exc
                await asyncio.sleep(2 ** attempt)
        raise AssertionError("unreachable")


class ExoplanetArchive:
    def __init__(self, tap: TapClient):
        self.tap = tap

    async def fetch(self, *, name: str | None = None) -> list[dict[str, Any]]:
        schema = await self.tap.query(NEA_TAP, "SELECT column_name FROM TAP_SCHEMA.columns WHERE table_name='pscomppars'")
        available = {r["column_name"].lower() for r in schema}
        desired = BASE_COLUMNS + [m + suffix for m in MEASURES for suffix in ("", "err1", "err2", "lim", "_reflink")]
        if not {"pl_name", "hostname", "ra", "dec"} <= available:
            raise ArchiveError("Exoplanet Archive schema lacks required identity columns")
        columns = [c for c in desired if c in available]
        conditions = []
        if name is not None:
            value = literal(name.strip())
            conditions.append("(" + " OR ".join(f"{c}={value}" for c in ("pl_name", *ALIAS_COLUMNS) if c in available) + ")")
        where = " WHERE " + " AND ".join(conditions) if conditions else ""
        expected = await self.tap.query(NEA_TAP, "SELECT count(*) AS n FROM pscomppars" + where)
        total = int(expected[0]["n"])
        # This service can apply TOP before ORDER BY. Keyset paging would silently
        # skip records. Request the bounded complete catalog and verify its count.
        if total > 100000:
            raise ArchiveError("Catalog exceeds the 100,000-row synchronous snapshot limit")
        result = await self.tap.query(NEA_TAP, f"SELECT {','.join(columns)} FROM pscomppars{where} ORDER BY pl_name", maxrec=total + 1)
        if len(result) != total or len({r["pl_name"] for r in result}) != total:
            raise ArchiveError(f"Archive changed or truncated during download: expected {total}, received {len(result)}, "
                               f"unique {len({r['pl_name'] for r in result})}; retry the snapshot")
        return result

    async def system(self, name: str) -> list[dict[str, Any]]:
        rows = await self.fetch(name=name)
        hosts = sorted({r["hostname"] for r in rows})
        if len(hosts) == 1 and any(r["pl_name"] == name for r in rows):
            return await self.fetch(name=hosts[0])
        return rows


def aliases(planets: list[dict[str, Any]]) -> list[str]:
    return sorted({str(p[c]).strip() for p in planets for c in ALIAS_COLUMNS if p.get(c)})


async def match_hosts(tap: TapClient, planets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    hosts = defaultdict(list)
    for p in planets:
        hosts[p["hostname"]].append(p)
    names = aliases(planets)
    matched = defaultdict(dict)
    for offset in range(0, len(names), 100):
        batch = names[offset:offset + 100]
        query = ("SELECT i.id,b.oid,b.main_id,b.ra,b.dec,b.otype,b.sp_type,b.plx_value,b.pmra,b.pmdec "
                 "FROM ident AS i JOIN basic AS b ON i.oidref=b.oid WHERE i.id IN (" +
                 ",".join(literal(n) for n in batch) + ")")
        rows = await tap.query(SIMBAD_TAP, query)
        for row in rows:
            matched[re.sub(r"\s+", " ", row["id"].strip())][str(row["oid"])] = row
    result = {}
    for host, members in sorted(hosts.items()):
        candidates = {}
        for alias in aliases(members):
            for oid, row in matched[re.sub(r"\s+", " ", alias)].items():
                if oid not in candidates:
                    candidates[oid] = {"record": {k: v for k, v in row.items() if k != "id"}, "matched_aliases": []}
                candidates[oid]["matched_aliases"].append(alias)
        result[host] = {"status": "matched" if len(candidates) == 1 else "ambiguous" if candidates else "unmatched",
                        "method": "exact_identifier", "candidates": sorted(candidates.values(), key=lambda c: str(c["record"]["oid"]))}
    return result


def measurement(row: dict[str, Any], field: str) -> dict[str, Any] | None:
    value = row.get(field)
    if value is None:
        return None
    label, unit = MEASURES[field]
    return {"field": field, "label": label, "value": value, "unit": unit,
            "error_plus": row.get(field + "err1"), "error_minus": row.get(field + "err2"),
            "limit": row.get(field + "lim"), "reference": row.get(field + "_reflink")}


def planet_summary(row: dict[str, Any]) -> dict[str, Any]:
    measures = [m for f in MEASURES if (m := measurement(row, f)) is not None]
    sentences = [f"{row['pl_name']} is listed in the NASA Exoplanet Archive around {row['hostname']}."]
    if row.get("discoverymethod"):
        year = f" in {int(row['disc_year'])}" if row.get("disc_year") else ""
        sentences.append(f"Discovery method: {row['discoverymethod']}{year}.")
    for m in measures:
        bound = {1: "upper limit ", -1: "lower limit "}.get(m["limit"], "")
        errors = ""
        if m["error_plus"] is not None or m["error_minus"] is not None:
            plus = str(m["error_plus"]) if m["error_plus"] is not None else "unknown"
            minus = str(m["error_minus"]) if m["error_minus"] is not None else "unknown"
            errors = f" (upper error {plus}, lower error {minus})"
        sentences.append(f"{m['label'].capitalize()}: {bound}{m['value']} {m['unit']}{errors}.")
    if row.get("pl_bmassprov"):
        sentences.append(f"Mass parameter provenance: {row['pl_bmassprov']}.")
    warnings = ["Composite parameters may come from different publications and need not be a self-consistent solution.",
                "Missing measurements are unknown; equilibrium temperature alone does not establish habitability."]
    if row.get("pl_controv_flag") == 1:
        warnings.append("The archive flags this planet's confirmation as controversial.")
    return {"id": entity_id("planet", row["pl_name"]), "name": row["pl_name"], "host": row["hostname"],
            "text": " ".join(sentences), "measurements": measures, "warnings": warnings,
            "source_url": "https://exoplanetarchive.ipac.caltech.edu/overview/" + quote(row["pl_name"], safe=""),
            "source_updated_at": row.get("rowupdate"), "discovery_reference": row.get("disc_refname"), "record": row}


def system_summary(planets: list[dict[str, Any]], matches: dict[str, dict[str, Any]]) -> dict[str, Any]:
    groups = defaultdict(list)
    for p in planets:
        groups[p["hostname"]].append(p)
    systems = []
    for name, members in sorted(groups.items()):
        crossref = matches.get(name, {"status": "unavailable", "candidates": []})
        summaries = [planet_summary(p) for p in sorted(members, key=lambda p: p["pl_name"])]
        text = f"{name}: {len(members)} planet records in this selection. "
        text += f"SIMBAD host cross-reference: {crossref['status']}."
        if crossref["status"] == "matched":
            star = crossref["candidates"][0]["record"]
            text += f" SIMBAD identifier: {star['main_id']}; object type: {star.get('otype') or 'unknown'}."
            if star.get("sp_type"):
                text += f" Spectral type: {star['sp_type']}."
        systems.append({"id": entity_id("host", name), "name": name, "text": text,
                        "planet_count": len(members), "planets": summaries, "simbad": crossref,
                        "warnings": ["Host matches do not identify planets as the same object as their stars.",
                                     "Identifier matches are catalog associations, not independent physical confirmation."]})
    return {"schema_version": SCHEMA_VERSION, "generated_at": utcnow(), "systems": systems}


def object_summary(result: dict[str, Any]) -> dict[str, Any]:
    resolved = result.get("resolved_object") or {}
    name = resolved.get("canonical_name") or "Coordinate target"
    target = result.get("target", {})
    counterparts = result.get("counterparts", {})
    counts = {wave: len(rows) for wave, rows in counterparts.items()}
    failures = result.get("failures", [])
    text = f"{name}: RA {target.get('ra', 'unknown')} deg, Dec {target.get('dec', 'unknown')} deg. "
    text += f"Queried {result.get('catalogs_queried', 0)} catalogs; returned {sum(counts.values())} candidate counterparts. "
    text += f"{len(failures)} catalog queries failed."
    candidates = []
    for wave, rows in counterparts.items():
        for row in rows:
            physical = row.get("physical") or {}
            candidate = {"catalog": row.get("catalog"), "source_id": row.get("source_id"), "wavelength": wave,
                         "separation_arcsec": row.get("separation_arcsec"), "match_score": row.get("confidence"),
                         "object_type": physical.get("object_type"), "spectral_type": physical.get("spectral_type"),
                         "links": row.get("links", {}), "provenance": row.get("provenance", {})}
            candidates.append(candidate)
    for candidate in candidates[:5]:
        text += f" Candidate {candidate['source_id'] or 'unnamed'} in {candidate['catalog'] or 'unknown catalog'}"
        if candidate["separation_arcsec"] is not None:
            text += f" lies {candidate['separation_arcsec']:.3g} arcsec from the query position"
        if candidate["object_type"]:
            text += f"; catalog object type {candidate['object_type']}"
        if candidate["spectral_type"]:
            text += f"; spectral type {candidate['spectral_type']}"
        text += "."
    return {"schema_version": SCHEMA_VERSION, "name": name, "text": text, "counts_by_wavelength": counts,
            "candidates": candidates,
            "status": "partial" if failures else "complete", "failures": failures,
            "warnings": ["Cone-search counterparts are candidates; proximity does not establish object identity.",
                         "No detections in queried catalogs does not prove an object is absent."],
            "evidence": result}


async def summarize_system(client: httpx.AsyncClient, name: str) -> dict[str, Any]:
    tap = TapClient(client)
    planets = await ExoplanetArchive(tap).system(name)
    if not planets:
        raise LookupError(f"No Exoplanet Archive system found for {name}")
    matches = await match_hosts(tap, planets)
    result = system_summary(planets, matches)
    result["provenance"] = tap.provenance
    return result

# ===== snapshots and Mantis publishing =====
MAP_FIELDS = ["catalog_id", "title", "kind", "host_id", "summary", "source", "source_url", "match_status",
              "ra_deg", "dec_deg", "planet_count", "discovery_method", "period_days", "radius_earth",
              "mass_earth", "distance_pc", "source_updated_at"]


@contextmanager
def state_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    path = root / ".sync.lock"
    try:
        handle = path.open("x", encoding="utf-8")
    except FileExistsError as exc:
        raise RuntimeError(
            "Another sync/publication holds .sync.lock; after a crashed run, verify no job is running before removing it"
        ) from exc
    try:
        with handle:
            handle.write(str(os.getpid()))
        yield
    finally:
        path.unlink(missing_ok=True)


def write_json(path: Path, value: Any):
    temp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    try:
        temp.write_text(canonical_json(value) + "\n", encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def load_json(path: Path, default=None):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def map_rows(summary: dict[str, Any]) -> list[dict[str, Any]]:
    rows, seen_simbad = [], set()
    for system in summary["systems"]:
        members = system["planets"]
        first = members[0]["record"]
        crossref = system["simbad"]
        rows.append({"catalog_id": system["id"], "title": system["name"], "kind": "host_system",
                     "host_id": system["id"], "summary": system["text"], "source": "NASA Exoplanet Archive + SIMBAD",
                     "source_url": "https://exoplanetarchive.ipac.caltech.edu/overview/" + quote(system["name"], safe=""),
                     "match_status": crossref["status"], "planet_count": len(members),
                     "ra_deg": first.get("ra"), "dec_deg": first.get("dec"), "distance_pc": first.get("sy_dist")})
        for p in members:
            raw = p["record"]
            rows.append({"catalog_id": p["id"], "title": p["name"], "kind": "planet", "host_id": system["id"],
                         "summary": p["text"] + " " + " ".join(p["warnings"]), "source": "NASA Exoplanet Archive",
                         "source_url": p["source_url"], "source_updated_at": p["source_updated_at"],
                         "match_status": "host_" + crossref["status"], "ra_deg": raw.get("ra"), "dec_deg": raw.get("dec"),
                         "discovery_method": raw.get("discoverymethod"), "period_days": raw.get("pl_orbper"),
                         "radius_earth": raw.get("pl_rade"), "mass_earth": raw.get("pl_bmasse"), "distance_pc": raw.get("sy_dist")})
        for candidate in crossref["candidates"]:
            star = candidate["record"]
            sid = entity_id("simbad", str(star["oid"]))
            if sid in seen_simbad:
                continue
            seen_simbad.add(sid)
            rows.append({"catalog_id": sid, "title": star["main_id"], "kind": "simbad_object", "source": "SIMBAD",
                         "summary": f"{star['main_id']}. SIMBAD type {star.get('otype') or 'unknown'}. "
                                    f"Spectral type {star.get('sp_type') or 'unknown'}. "
                                    "Identifier cross-reference candidate for exoplanet host records; see relationships.json.",
                         "source_url": "https://simbad.cds.unistra.fr/simbad/sim-id?Ident=" + quote(star["main_id"], safe=""),
                         "match_status": "candidate", "ra_deg": star.get("ra"), "dec_deg": star.get("dec")})
    return sorted(rows, key=lambda r: r["catalog_id"])


def relationships(summary: dict[str, Any]) -> list[dict[str, Any]]:
    edges = []
    for s in summary["systems"]:
        for p in s["planets"]:
            edges.append({"from": p["id"], "to": s["id"], "relation": "orbits_host", "source": "NASA Exoplanet Archive"})
        for candidate in s["simbad"]["candidates"]:
            edges.append({"from": s["id"], "to": entity_id("simbad", str(candidate["record"]["oid"])),
                          "relation": "identifier_match" if s["simbad"]["status"] == "matched" else "ambiguous_candidate",
                          "source": "SIMBAD ident", "aliases": candidate["matched_aliases"]})
    return edges


def commit_snapshot(root: Path, planets: list[dict[str, Any]], matches: dict[str, Any], provenance: list[dict[str, Any]]) -> dict[str, Any]:
    if not planets:
        raise ArchiveError("Refusing to replace a catalog snapshot with an empty result")
    planets = sorted(planets, key=lambda p: p["pl_name"])
    payload = {"schema_version": SCHEMA_VERSION, "planets": planets, "matches": matches}
    fingerprint = digest(payload)
    previous = load_json(root / "latest.json")
    if previous and previous["fingerprint"] == fingerprint:
        write_json(root / "last-check.json", {"checked_at": utcnow(), "changed": False, "fingerprint": fingerprint,
                                             "provenance": provenance})
        return {**previous, "changed": False}
    prior = load_json(root / "snapshots" / previous["fingerprint"] / "catalog.json", {}) if previous else {}
    old = {p["pl_name"]: digest(p) for p in prior.get("planets", [])}
    new = {p["pl_name"]: digest(p) for p in planets}
    changes = {"added": sorted(new.keys() - old.keys()), "removed": sorted(old.keys() - new.keys()),
               "updated": sorted(k for k in new.keys() & old.keys() if old[k] != new[k]),
               "simbad_changed": prior.get("matches") != matches}
    final = root / "snapshots" / fingerprint
    stage = root / "snapshots" / (".staging-" + uuid.uuid4().hex)
    summary = system_summary(planets, matches)
    rows = map_rows(summary)
    manifest = {"schema_version": SCHEMA_VERSION, "fingerprint": fingerprint, "created_at": utcnow(),
                "planet_count": len(planets), "host_count": len(matches), "map_rows": len(rows),
                "match_counts": dict(Counter(m["status"] for m in matches.values())), "changes": changes,
                "provenance": provenance}
    try:
        stage.mkdir(parents=True)
        for filename, data in {"catalog.json": payload, "summaries.json": summary,
                               "relationships.json": relationships(summary), "manifest.json": manifest}.items():
            write_json(stage / filename, data)
        with (stage / "mantis.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=MAP_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
        if final.exists():
            shutil.rmtree(stage)
        else:
            stage.rename(final)
        write_json(root / "latest.json", manifest)
        write_json(root / "last-check.json", {"checked_at": utcnow(), "changed": True, "fingerprint": fingerprint})
    except BaseException:
        if stage.exists():
            shutil.rmtree(stage)
        raise
    return {**manifest, "changed": True}


async def sync(root: Path) -> dict[str, Any]:
    with state_lock(root):
        async with httpx.AsyncClient(follow_redirects=True) as client:
            tap = TapClient(client)
            planets = await ExoplanetArchive(tap).fetch()
            matches = await match_hosts(tap, planets)
            return commit_snapshot(root, planets, matches, tap.provenance)


def mantis_command(
    csv_path: Path,
    fingerprint: str,
    *,
    space_id: str | None = None,
    space_name: str = "Astronomical Catalogs",
) -> list[str]:
    command = ["mantis", "create", "map", str(csv_path), "--space-mode", "existing" if space_id else "new"]
    if space_id:
        command.extend(["--space-id", str(uuid.UUID(space_id))])
    else:
        command.extend(["--space-name", space_name, "--private"])
    command.extend(["--map-name", f"Exoplanets and SIMBAD {fingerprint[:12]}", "--title-column", "title",
                    "--semantic-column", "summary", "--categoric-column", "catalog_id,kind,host_id,source,match_status,discovery_method",
                    "--numeric-column", "ra_deg,dec_deg,planet_count,period_days,radius_earth,mass_earth,distance_pc",
                    "--date-column", "source_updated_at", "--links-column", "source_url", "--no-activate"])
    return command


def run_mantis(arguments: list[str], *, timeout: int) -> subprocess.CompletedProcess:
    executable = shutil.which("mantis")
    if not executable:
        raise RuntimeError("MIT CSAIL Mantis CLI is missing; install mantisai-cli and run mantis setup")
    # On Windows invoke the installed Node entry point directly, avoiding cmd.exe quoting.
    if os.name == "nt":
        script = Path(executable).parent / "node_modules" / "mantisai-cli" / "dist" / "mantis.js"
        node = shutil.which("node")
        if not node or not script.exists():
            raise RuntimeError("Cannot locate the installed Mantis Node entry point")
        argv = [node, str(script), *arguments]
    else:
        argv = [executable, *arguments]
    return subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", timeout=timeout, check=False)


def publish(root: Path, *, space_id: str | None = None, space_name: str = "Astronomical Catalogs") -> dict[str, Any]:
    with state_lock(root):
        latest = load_json(root / "latest.json")
        if not latest:
            raise RuntimeError("Run sync before publishing")
        fingerprint = latest["fingerprint"]
        path = root / "publications.json"
        publications = load_json(path, {})
        target = space_id or "new:" + space_name
        key = target + ":" + fingerprint
        if key in publications:
            saved = publications[key]
            if saved["status"] not in {"submitted", "complete"}:
                raise RuntimeError(
                    "Previous publication has an uncertain outcome. Inspect Mantis before reconciling "
                    "publications.json; automatic retry could duplicate a map"
                )
            return saved
        context = run_mantis(["use", "get_space_context"], timeout=60)
        if context.returncode:
            # MCP and REST have separate authentication paths. Map creation uses
            # REST, so an unavailable MCP session must not block a valid REST key.
            rest = run_mantis(["spaces", "list", "--filter", space_name], timeout=60)
            if rest.returncode:
                raise RuntimeError("Mantis authentication failed for both MCP and REST. Reconnect locally with mantis setup")
        targets_path = root / "mantis-targets.json"
        targets = load_json(targets_path, {})
        actual_space = space_id or targets.get(target)
        command = mantis_command(root / "snapshots" / fingerprint / "mantis.csv", fingerprint, space_id=actual_space, space_name=space_name)
        publications[key] = {"status": "pending", "started_at": utcnow(), "fingerprint": fingerprint}
        write_json(path, publications)
        response = run_mantis(command[1:], timeout=1800)
        if response.returncode:
            raise RuntimeError("Mantis publication failed or has an uncertain outcome. Inspect the space before retrying")
        # The CLI submits asynchronous embedding; an exit code alone is not evidence
        # that the map finished building. Preserve server identifiers for inspection.
        decoded = None
        for offset, character in enumerate(response.stdout):
            if character == "{":
                try:
                    value, _ = json.JSONDecoder().raw_decode(response.stdout[offset:])
                    if isinstance(value, dict) and value.get("space_id") and value.get("map_id"):
                        decoded = value
                except ValueError:
                    pass
        if decoded is None:
            raise RuntimeError(
                "Mantis accepted the request but its identifiers could not be parsed; reconcile the pending "
                "publication before retrying"
            )
        targets[target] = str(uuid.UUID(decoded["space_id"]))
        write_json(targets_path, targets)
        publications[key].update({"status": "submitted", "submitted_at": utcnow(), "result": decoded,
                                  "space_url": "https://mantis.csail.mit.edu/space/" + targets[target]})
        write_json(path, publications)
        return publications[key]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    summarize = sub.add_parser("summarize", help="Summarize an extrasolar host or planet and its system")
    summarize.add_argument("name")
    general = sub.add_parser("summarize-object", help="Summarize a general astronomical object using catalog search")
    general.add_argument("name")
    general.add_argument("--profile", default="optical")
    synchronize = sub.add_parser("sync", help="Download complete exoplanet catalog and cross-reference SIMBAD hosts")
    synchronize.add_argument("--state-dir", type=Path, default=Path("astronomy-data"))
    publication = sub.add_parser("publish", help="Publish the latest snapshot as a private Mantis map")
    publication.add_argument("--state-dir", type=Path, default=Path("astronomy-data"))
    publication.add_argument("--space-id")
    publication.add_argument("--space-name", default="Astronomical Catalogs")
    publication.add_argument("--dry-run", action="store_true")
    watch = sub.add_parser("watch", help="Continuously refresh snapshots; optionally publish changed maps")
    watch.add_argument("--state-dir", type=Path, default=Path("astronomy-data"))
    watch.add_argument("--interval-hours", type=float, default=24)
    watch.add_argument("--publish", action="store_true")
    watch.add_argument("--space-id")
    arguments = parser.parse_args()
    try:
        if arguments.command == "summarize":
            async def run():
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    return await summarize_system(client, arguments.name)
            result = asyncio.run(run())
        elif arguments.command == "summarize-object":
            from main import search_object
            result = object_summary(asyncio.run(search_object(arguments.name, profile=arguments.profile)).as_dict())
        elif arguments.command == "sync":
            result = asyncio.run(sync(arguments.state_dir.resolve()))
        elif arguments.command == "watch":
            if not 1 <= arguments.interval_hours <= 8760:
                raise ValueError("interval-hours must be between 1 and 8760")
            while True:
                try:
                    result = asyncio.run(sync(arguments.state_dir.resolve()))
                    if arguments.publish:
                        publication_result = publish(arguments.state_dir.resolve(), space_id=arguments.space_id)
                        result["publication"] = publication_result
                    print(json.dumps({"checked_at": utcnow(), "changed": result["changed"],
                                      "fingerprint": result["fingerprint"]}), flush=True)
                except (ArchiveError, RuntimeError, OSError, subprocess.TimeoutExpired) as exc:
                    print(json.dumps({"checked_at": utcnow(), "error": str(exc)}), file=sys.stderr, flush=True)
                time.sleep(arguments.interval_hours * 3600)
        elif arguments.dry_run:
            root = arguments.state_dir.resolve()
            latest = load_json(root / "latest.json")
            if not latest:
                raise RuntimeError("Run sync first")
            fingerprint = latest["fingerprint"]
            result = {"argv": mantis_command(root / "snapshots" / fingerprint / "mantis.csv", fingerprint,
                                             space_id=arguments.space_id, space_name=arguments.space_name)}
        else:
            result = publish(arguments.state_dir.resolve(), space_id=arguments.space_id, space_name=arguments.space_name)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    except (ArchiveError, LookupError, ValueError, RuntimeError, subprocess.TimeoutExpired) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc

# ===== catalog source plotting =====
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt



def _decode_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _read_source_file(path: Path) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix == ".csv":
        with path.open(newline="", encoding="utf-8-sig") as handle:
            rows = list(csv.DictReader(handle))
    elif suffix == ".parquet":
        import pyarrow.parquet as pq

        rows = pq.read_table(path).to_pylist()
    elif suffix in {".fits", ".fit", ".fts"}:
        from astropy.table import Table

        rows = Table.read(path).to_pandas().to_dict(orient="records")
    else:
        raise ValueError("Input must be a JSON, CSV, Parquet, or FITS crossmatch export")

    for row in rows:
        for key in ("data", "metadata", "physical", "provenance", "links"):
            row[key] = _decode_mapping(row.get(key))
    return rows


def _extract_sources(payload: Any) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """Accept a UnifiedRecord, a list of UnifiedRecords, or exported source rows."""
    if isinstance(payload, list):
        targets: list[dict[str, Any]] = []
        sources: list[dict[str, Any]] = []
        for item in payload:
            target, found = _extract_sources(item)
            if target:
                targets.append(target)
            sources.extend(found)
        return (targets[0] if targets else None), sources

    if not isinstance(payload, dict):
        raise ValueError("Input JSON must contain a crossmatch object or source rows")

    target = payload.get("target") if isinstance(payload.get("target"), dict) else None
    sources: list[dict[str, Any]] = []
    catalog_results = payload.get("catalog_results")
    if isinstance(catalog_results, dict):
        for catalog_name, result in catalog_results.items():
            if not isinstance(result, dict):
                continue
            for source in result.get("sources", []):
                if isinstance(source, dict):
                    item = dict(source)
                    item.setdefault("catalog", catalog_name)
                    sources.append(item)
    elif isinstance(payload.get("counterparts"), dict):
        for group in payload["counterparts"].values():
            if isinstance(group, list):
                sources.extend(item for item in group if isinstance(item, dict))
    elif isinstance(payload.get("sources"), list):
        sources.extend(item for item in payload["sources"] if isinstance(item, dict))
    elif payload.get("catalog"):
        sources.append(payload)
    return target, sources


def _wavelength(source: dict[str, Any], registry: dict[str, Any]) -> str:
    metadata = _decode_mapping(source.get("metadata"))
    wave = str(source.get("wavelength") or metadata.get("wavelength") or "").lower()
    if wave:
        return wave
    catalog = str(source.get("catalog") or "")
    definition = registry.get(catalog)
    return definition.wavelength.lower() if definition else "unknown"


def _as_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _normalize_sources(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    registry = CatalogRegistry().catalogs
    selected: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float, float]] = set()
    for source in sources:
        wave = _wavelength(source, registry)
        if wave not in {"infrared", "radio"}:
            continue
        ra = _as_float(source.get("ra"))
        dec = _as_float(source.get("dec"))
        if ra is None or dec is None or not -90 <= dec <= 90:
            continue
        catalog = str(source.get("catalog") or "unknown")
        source_id = str(source.get("source_id") or source.get("id") or "unknown")
        key = (catalog, source_id, ra, dec)
        if key in seen:
            continue
        seen.add(key)
        item = dict(source)
        item["catalog"] = catalog
        item["source_id"] = source_id
        item["wavelength"] = wave
        item["ra"] = ra
        item["dec"] = dec
        item["data"] = _decode_mapping(source.get("data"))
        selected.append(item)
    return selected


def _center(target: dict[str, Any] | None, sources: list[dict[str, Any]]) -> tuple[float, float]:
    if target:
        ra, dec = _as_float(target.get("ra")), _as_float(target.get("dec"))
        if ra is not None and dec is not None:
            return ra, dec
    if not sources:
        return 0.0, 0.0
    # Circular mean handles data close to the 0/360 degree RA boundary.
    angles = np.deg2rad([source["ra"] for source in sources])
    ra = math.degrees(math.atan2(float(np.sin(angles).mean()), float(np.cos(angles).mean()))) % 360.0
    dec = float(np.mean([source["dec"] for source in sources]))
    return ra, dec


def _plot_positions(sources: list[dict[str, Any]], target: dict[str, Any] | None, out_dir: Path) -> Path:
    center_ra, center_dec = _center(target, sources)
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in sources:
        groups[source["catalog"]].append(source)

    fig, ax = plt.subplots(figsize=(10, 8))
    for catalog, group in sorted(groups.items()):
        east = [(((s["ra"] - center_ra + 180.0) % 360.0) - 180.0) * math.cos(math.radians(center_dec)) * 3600.0 for s in group]
        north = [(s["dec"] - center_dec) * 3600.0 for s in group]
        wave = group[0]["wavelength"]
        ax.scatter(east, north, s=55, alpha=0.82, label=f"{catalog} ({wave}, {len(group)})")
    reference_label = "query target" if target else "sample center"
    if target or groups:
        ax.scatter([0], [0], marker="+", s=150, linewidths=2.2, color="black", label=reference_label, zorder=5)
    ax.axhline(0, color="0.85", linewidth=0.8)
    ax.axvline(0, color="0.85", linewidth=0.8)
    ax.invert_xaxis()  # Astronomical convention: east appears to the left.
    ax.set_aspect("equal", adjustable="datalim")
    ax.set_xlabel(f"Offset east of {reference_label} (arcsec; east to the left)")
    ax.set_ylabel(f"Offset north of {reference_label} (arcsec)")
    ax.set_title("Infrared and radio catalog detections")
    if groups or target:
        ax.legend(loc="best", fontsize="small")
    fig.tight_layout()
    path = out_dir / "source_positions.png"
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


_IR_MAGNITUDE = re.compile(r"^(j|h|k|w[1-4])_?m(?:pro|ag)?$", re.IGNORECASE)
_RADIO_FLUX = re.compile(r"(?:flux|fpeak|fint|peakflux|intflux)", re.IGNORECASE)
_NON_MEASUREMENT = re.compile(r"(?:err|sig|snr|rms|noise|flag|qual|limit|upper|chisq|chi2)", re.IGNORECASE)


def _measurements(source: dict[str, Any], wave: str) -> list[tuple[str, float]]:
    data = source.get("data") if isinstance(source.get("data"), dict) else {}
    result = []
    for key, value in data.items():
        name = str(key).strip()
        if _NON_MEASUREMENT.search(name):
            continue
        if wave == "infrared":
            match = _IR_MAGNITUDE.fullmatch(name)
            if not match:
                continue
            number = _as_float(value)
            if number is None or not -100 <= number <= 100:
                continue
            label = match.group(1).upper()
        else:
            if not _RADIO_FLUX.search(name):
                continue
            number = _as_float(value)
            if number is None:
                continue
            label = name
        result.append((label, number))
    return result


def _plot_measurements(sources: list[dict[str, Any]], wave: str, out_dir: Path) -> Path | None:
    rows: list[tuple[int, str, float]] = []
    relevant = [source for source in sources if source["wavelength"] == wave]
    for index, source in enumerate(relevant):
        rows.extend((index, label, value) for label, value in _measurements(source, wave))
    if not rows:
        return None

    labels = [f"{source['catalog']}\n{source['source_id']}" for source in relevant]
    series: dict[str, list[tuple[int, float]]] = defaultdict(list)
    for index, label, value in rows:
        series[label].append((index, value))
    width = max(10.0, min(28.0, len(relevant) * 0.55))
    fig, ax = plt.subplots(figsize=(width, 7))
    for label, points in sorted(series.items()):
        ax.scatter([point[0] for point in points], [point[1] for point in points], s=48, alpha=0.82, label=label)
    ax.set_xticks(range(len(relevant)), labels, rotation=65, ha="right", fontsize="small")
    if wave == "infrared":
        ax.set_ylabel("Reported magnitude (mag; lower values are brighter)")
        ax.set_title("Infrared magnitudes by catalog detection")
        ax.invert_yaxis()
        filename = "infrared_magnitudes.png"
    else:
        ax.set_ylabel("Reported flux value (native catalog units; no unit conversion)")
        ax.set_title("Radio flux measurements by catalog detection")
        filename = "radio_flux_measurements.png"
    ax.set_xlabel("Catalog and source ID")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Measurement field", loc="best", fontsize="small")
    fig.tight_layout()
    path = out_dir / filename
    fig.savefig(path, dpi=180)
    plt.close(fig)
    return path


async def _query_profiles(args: argparse.Namespace) -> list[dict[str, Any]]:
    results = []
    for profile in args.profiles:
        if args.name:
            result = await search_object(args.name, radius_arcsec=args.radius, profile=profile)
        else:
            result = await crossmatch(args.ra, args.dec, radius_arcsec=args.radius, profile=profile)
        results.append(result.as_dict())
    return results


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Saved UnifiedRecord or dataset export (JSON, CSV, Parquet, FITS)")
    parser.add_argument("--name", help="Object name to resolve and query")
    parser.add_argument("--ra", type=float, help="Right ascension in degrees")
    parser.add_argument("--dec", type=float, help="Declination in degrees")
    parser.add_argument("--radius", type=float, default=3.0, help="Catalog search radius in arcseconds")
    parser.add_argument("--profiles", nargs="+", choices=("infrared", "radio"), default=["infrared", "radio"],
                        help="Catalog profiles to query; defaults to both infrared and radio")
    parser.add_argument("--output-dir", type=Path, default=Path("catalog-plots"), help="Directory for PNG plots and query JSON")
    args = parser.parse_args(argv)

    coordinate_pair = args.ra is not None and args.dec is not None
    if args.input and (args.name or args.ra is not None or args.dec is not None):
        parser.error("--input cannot be combined with --name, --ra, or --dec")
    if not args.input and not args.name and not coordinate_pair:
        parser.error("provide --input, --name, or both --ra and --dec")
    if args.name and (args.ra is not None or args.dec is not None):
        parser.error("--name cannot be combined with --ra or --dec")
    if (args.ra is None) != (args.dec is None):
        parser.error("--ra and --dec must be supplied together")
    if not math.isfinite(args.radius) or args.radius <= 0:
        parser.error("--radius must be finite and greater than zero")
    return args


def plot_main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    try:
        if args.input:
            payload = _read_source_file(args.input)
        else:
            payload = asyncio.run(_query_profiles(args))
            args.output_dir.mkdir(parents=True, exist_ok=True)
            (args.output_dir / "crossmatch-results.json").write_text(
                json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8"
            )
        target, raw_sources = _extract_sources(payload)
        sources = _normalize_sources(raw_sources)
        args.output_dir.mkdir(parents=True, exist_ok=True)
        outputs = [_plot_positions(sources, target, args.output_dir)]
        for wave in ("infrared", "radio"):
            plot = _plot_measurements(sources, wave, args.output_dir)
            if plot:
                outputs.append(plot)
        counts = defaultdict(int)
        for source in sources:
            counts[source["wavelength"]] += 1
        print(f"Plotted {len(sources)} detections: {counts['infrared']} infrared, {counts['radio']} radio")
        for output in outputs:
            print(output)
        if not sources:
            print("No infrared or radio detections with usable coordinates were found in the input/query result.", file=sys.stderr)
        return 0
    except (OSError, ValueError, ImportError) as exc:
        print(f"astrosearch-plot-catalogs: {exc}", file=sys.stderr)
        return 2

if __name__ == "__main__":
    main()
