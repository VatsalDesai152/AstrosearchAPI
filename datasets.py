"""Streaming multi-format dataset generation, storage backends (SQLite/PostgreSQL/S3), and worker jobs."""



from __future__ import annotations

import asyncio
import csv
import hashlib
import json
import math
import os
import re
import sqlite3
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal, Self, TextIO

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import structlog
from prometheus_client import Counter, Gauge, Histogram
from pydantic import BaseModel, ConfigDict, Field, model_validator

from core import (
    AdvancedQuery,
    CatalogRegistry,
    CatalogSource,
    CrossmatchService,
    InvalidCoordinateError,
    ProviderBudget,
    QueryBuilder,
    QueryValidator,
    Target,
    angular_separation_arcsec,
    budgeted_requests_adapter,
    match_score,
    normalize_source_record,
    provider_identity,
    provider_map,
    validate_target,
)

logger = structlog.get_logger()

BUILDS = Counter("astrosearch_dataset_builds_total", "Dataset outcomes", ["status"])
RECORDS = Counter("astrosearch_dataset_records_total", "Dataset record counts", ["kind"])
REUSE = Histogram("astrosearch_dataset_reuse_ratio", "Fraction of selected records satisfied locally")
MATERIALIZE = Histogram("astrosearch_materialization_seconds", "Dataset materialization time", ["format"])
OUTPUT_BYTES = Counter("astrosearch_dataset_bytes_total", "Dataset artifact bytes", ["format"])
QUEUE_DEPTH = Gauge("astrosearch_archive_queue_depth", "RQ waiting shards", ["provider"])


class DatasetRequest(BaseModel):
    """What to fulfill; AdvancedQuery remains the geometric/filter query model.

    A count activates object fulfillment. Omitting count retains legacy detection exports.
    Qualified fields use ``catalog.field``; products currently support TESS SPOC light curves.
    """
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)
    name: str = Field("dataset", min_length=1, max_length=100)
    profile: str = "full"
    radius_arcsec: float = Field(10.0, gt=0)
    count: int | None = Field(None, gt=0, strict=True)
    fields: list[str] = Field(default_factory=list)
    products: list[Literal["tess_light_curve"]] = Field(default_factory=list)
    quality_constraints: dict[str, Any] = Field(default_factory=dict)
    intended_use: Literal["table", "astronomy", "analysis"] = "table"
    catalogs: list[str] | None = None
    filters: dict[str, Any] | None = None
    targets: list[dict[str, Any]] = Field(default_factory=list)
    object_types: list[str] | None = None
    time_period: dict[str, Any] | None = None
    count_threshold: int = Field(1, gt=0)
    min_confidence: float = Field(0.0, ge=0, le=1)
    max_results: int | None = Field(None, gt=0)
    output_format: Literal["csv", "parquet", "fits", "json", "jsonl"] | None = None
    export_path: str | None = None

    @model_validator(mode="after")
    def normalized(self):
        if not self.name.strip() or not self.profile.strip():
            raise ValueError("name and profile cannot be blank")
        if self.count is None and (self.fields or self.products or self.quality_constraints):
            raise ValueError("fields/products/quality_constraints require count")
        if self.count is not None and self.max_results is not None:
            raise ValueError("count and max_results cannot be combined; count is the fulfillment limit")
        if self.count is None and not self.targets:
            raise ValueError("targets are required for legacy datasets")
        self.fields = sorted(set(self.fields))
        self.products = sorted(set(self.products))
        for target in self.targets:
            if "ra" not in target or "dec" not in target:
                raise ValueError("Each target requires ra and dec")
            validate_target(target["ra"], target["dec"], epoch=target.get("epoch"))
        for key in [*self.fields, *(self.filters or {}), *self.quality_constraints]:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*(\.[A-Za-z_][A-Za-z_0-9]*)?", key):
                raise ValueError(f"Invalid field name: {key}")
        return self

    def query(self, target=None):
        return AdvancedQuery.from_dict({**self.model_dump(), "target": target or (
            self.targets[0] if self.targets else {"ra": 0, "dec": 0}), "profiles": [self.profile]})

    def validate_registry(self, registry):
        QueryValidator.validate(self.query(), registry)
        selected = {p.catalog for p in QueryBuilder(registry).build(self.query())}
        if not selected:
            raise ValueError("No catalogs match the profile and catalog selection")
        for key in [*self.fields, *(self.filters or {}), *self.quality_constraints]:
            name = key.removeprefix("min_").removeprefix("max_")
            if "." in name and name.split(".", 1)[0] not in selected:
                raise ValueError(f"Field catalog is not selected: {name}")
        return selected


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def _present(value):
    if value is None or (isinstance(value, str) and not value):
        return False
    if isinstance(value, float) and not math.isfinite(value):
        return False
    return not (isinstance(value, (list, dict)) and not value)


def _source(source):
    """Rehydrate the existing canonical model; do not persist request-specific scores."""
    values = {k: source.get(k) for k in CatalogSource.__dataclass_fields__}
    for key in ("data", "metadata", "provenance"):
        values[key] = values[key] or {}
    return CatalogSource(**values)


def _values(member):
    return {**normalize_source_record(member.get("data") or {}),
            **(member.get("metadata", {}).get("physical") or {}),
            **(member.get("physical") or {}),
            **{k: v for k, v in member.items() if not isinstance(v, (list, dict))}}


def _field_value(record, name):
    catalog, sep, field_name = name.partition(".")
    for member in record["members"]:
        if not sep or member["catalog"] == catalog:
            value = _values(member).get(field_name if sep else name)
            if _present(value):
                return value
    return None

# ---------------------------------------------------------------------------
# Multi-Format Streaming Exporter
# ---------------------------------------------------------------------------

_TEXT_COLUMNS = ("catalog", "source_id", "physical", "data", "metadata", "provenance", "links")
_FLOAT_COLUMNS = ("ra", "dec", "separation_arcsec", "confidence", "epoch", "positional_error_arcsec")
_COLUMNS = _TEXT_COLUMNS + _FLOAT_COLUMNS


def _flat(source: dict[str, Any]) -> dict[str, Any]:
    return {
        key: json.dumps(source.get(key), default=str)
        if key in {"physical", "data", "metadata", "provenance", "links"}
        else str(source[key])
        if key in {"catalog", "source_id"} and key in source
        else float(source[key])
        if source.get(key) is not None and key in _FLOAT_COLUMNS
        else None
        for key in _COLUMNS
    }


class DatasetWriter:
    """Context manager for streaming multi-format dataset exports (JSON, CSV, Parquet, FITS)."""

    def __init__(self, path: Path, output_format: str, *, schema: pa.Schema | None = None) -> None:
        self.path = path
        self.output_format = output_format.lower()
        self.count = 0
        self._buffer: list[dict[str, Any]] = []
        self._handle: TextIO | None = None
        self._csv_writer: csv.DictWriter | None = None
        self._parquet_writer: pq.ParquetWriter | None = None
        self._schema: pa.Schema = schema if schema is not None else pa.schema([])
        self._projected = schema is not None

    def __enter__(self) -> Self:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self.output_format == "json":
            self._handle = self.path.open("w", encoding="utf-8")
            self._handle.write("[")
        elif self.output_format == "jsonl":
            self._handle = self.path.open("w", encoding="utf-8")
        elif self.output_format == "csv":
            self._handle = self.path.open("w", newline="", encoding="utf-8")
            self._csv_writer = csv.DictWriter(self._handle, fieldnames=self._schema.names if self._projected else _COLUMNS)
            self._csv_writer.writeheader()
        elif self.output_format == "parquet":
            self._schema = self._schema if self._projected else pa.schema(
                [(name, pa.string()) for name in _TEXT_COLUMNS]
                + [(name, pa.float64()) for name in _FLOAT_COLUMNS]
            )
            self._parquet_writer = pq.ParquetWriter(self.path, self._schema, compression="zstd")
        elif self.output_format != "fits":
            raise ValueError(f"Unsupported output format: {self.output_format}")
        return self

    def write(self, source: dict[str, Any]) -> None:
        """Write a single catalog detection to the dataset stream."""
        if self.output_format in {"json", "jsonl"}:
            assert self._handle is not None
            if self.count and self.output_format == "json":
                self._handle.write(",\n")
            json.dump(source, self._handle, default=str)
            if self.output_format == "jsonl":
                self._handle.write("\n")
        elif self.output_format == "csv":
            assert self._csv_writer is not None
            self._csv_writer.writerow(self._row(source))
        else:
            self._buffer.append(self._row(source))
            if self.output_format == "parquet" and len(self._buffer) >= 1000:
                self._flush_parquet()
        self.count += 1

    def _row(self, source):
        if not self._projected:
            return _flat(source)
        return {f.name: json.dumps(source.get(f.name), default=str) if isinstance(source.get(f.name), (list, dict))
                else str(source[f.name]) if pa.types.is_string(f.type) and source.get(f.name) is not None
                else source.get(f.name) for f in self._schema}

    def _flush_parquet(self) -> None:
        if self._buffer:
            assert self._parquet_writer is not None and self._schema is not None
            self._parquet_writer.write_table(pa.Table.from_pylist(self._buffer, schema=self._schema))
            self._buffer.clear()

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            if self.output_format == "json":
                assert self._handle is not None
                self._handle.write("]")
            elif self.output_format == "parquet" and exc_type is None:
                self._flush_parquet()
            elif self.output_format == "fits" and exc_type is None:
                from astropy.table import MaskedColumn, Table

                fits_rows = [
                    {
                        key: val if val is not None else float("nan") if (
                            key in _FLOAT_COLUMNS or self._projected and pa.types.is_floating(self._schema.field(key).type)) else ""
                        for key, val in row.items()
                    }
                    for row in self._buffer
                ]
                table = Table(rows=fits_rows) if fits_rows else Table(
                    names=self._schema.names if self._projected else _COLUMNS,
                    dtype=["f8" if pa.types.is_floating(f.type) else "U1" for f in self._schema] if self._projected else (
                        ["U1"] * len(_TEXT_COLUMNS) + ["f8"] * len(_FLOAT_COLUMNS)),
                )
                if self._projected:
                    columns = []
                    for column in self._schema:
                        values = [row[column.name] for row in self._buffer]
                        mask = [v is None for v in values]
                        dtype = "f8" if pa.types.is_floating(column.type) else "i8" if pa.types.is_integer(column.type) else (
                            "bool" if pa.types.is_boolean(column.type) else f"U{max([1, *(len(str(v)) for v in values if v is not None)])}")
                        default = "" if pa.types.is_string(column.type) else 0
                        columns.append(MaskedColumn([v if v is not None else default for v in values],
                                                    mask=mask, name=column.name, dtype=dtype))
                    table = Table(columns, masked=True)
                table.write(self.path, format="fits", overwrite=True)
        finally:
            if self._handle:
                self._handle.close()
            if self._parquet_writer:
                self._parquet_writer.close()
            if exc_type is not None:
                self.path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Storage Layer: MetadataStore and ObjectStore
# ---------------------------------------------------------------------------


class _SQLiteConnection(sqlite3.Connection):
    def __exit__(self, *args):
        try:
            return super().__exit__(*args)
        finally:
            self.close()


class MetadataStore:
    """Durable metadata and query history store supporting SQLite and PostgreSQL."""

    def __init__(self, database_url: str | None = None, *, local_dir: str | Path = "datasets") -> None:
        url = database_url or os.getenv("DATABASE_URL")
        self.postgres = bool(url and url.startswith(("postgresql://", "postgres://")))
        if url and not self.postgres and not url.startswith("sqlite:///"):
            raise ValueError("DATABASE_URL must use postgresql:// or sqlite:///")
        self.database_url = url
        self.sqlite_path = (
            Path(url.removeprefix("sqlite:///"))
            if url and not self.postgres
            else Path(local_dir) / "metadata.sqlite3"
        )
        if not self.postgres:
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.degraded = False
        try:
            self._initialize()
        except Exception:
            if not self.postgres or os.getenv("DATASET_EXECUTION_MODE", "local") == "rq":
                raise
            logger.warning("postgres_unavailable_using_local_store")
            self.postgres = False
            self.degraded = True
            self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _connect(self):
        if self.postgres:
            import psycopg
            return psycopg.connect(self.database_url)
        connection = sqlite3.connect(self.sqlite_path, timeout=30, factory=_SQLiteConnection)
        connection.execute("PRAGMA journal_mode=WAL")
        return connection

    @contextmanager
    def transaction(self):
        """Serialize identity assignment and task claims, with rollback on failure."""
        conn = self._connect()
        try:
            if self.postgres:
                conn.execute("SELECT id FROM canonical_lock WHERE id=1 FOR UPDATE")
            else:
                conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _sql(self, statement: str) -> str:
        return statement.replace("?", "%s") if self.postgres else statement

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                self._sql(
                    "CREATE TABLE IF NOT EXISTS datasets ("
                    "id TEXT PRIMARY KEY, metadata_json TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
            )
            conn.execute(
                self._sql(
                    "CREATE TABLE IF NOT EXISTS saved_queries ("
                    "id TEXT PRIMARY KEY, name TEXT NOT NULL, query_json TEXT NOT NULL, created_at TEXT NOT NULL)"
                )
            )
            for statement in (
                "CREATE TABLE IF NOT EXISTS canonical_lock (id INTEGER PRIMARY KEY)",
                "INSERT INTO canonical_lock (id) VALUES (1) ON CONFLICT (id) DO NOTHING",
                ("CREATE TABLE IF NOT EXISTS canonical_sources (catalog TEXT NOT NULL, source_id TEXT NOT NULL, "
                "object_id TEXT NOT NULL, ra DOUBLE PRECISION NOT NULL, dec DOUBLE PRECISION NOT NULL, "
                "source_json TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(catalog, source_id))"),
                "CREATE INDEX IF NOT EXISTS canonical_position ON canonical_sources(dec, ra)",
                "CREATE INDEX IF NOT EXISTS canonical_catalog_position ON canonical_sources(catalog, dec, ra)",
                "CREATE INDEX IF NOT EXISTS canonical_object ON canonical_sources(object_id)",
                ("CREATE TABLE IF NOT EXISTS canonical_products (object_id TEXT NOT NULL, product TEXT NOT NULL, "
                "product_json TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(object_id, product))"),
                ("CREATE TABLE IF NOT EXISTS dataset_work (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL, "
                "state TEXT NOT NULL, owner TEXT, lease_until DOUBLE PRECISION NOT NULL DEFAULT 0, "
                "attempts INTEGER NOT NULL DEFAULT 0, result_json TEXT)"),
                "CREATE TABLE IF NOT EXISTS archive_scans (id TEXT PRIMARY KEY, state_json TEXT NOT NULL, "
                "updated_at DOUBLE PRECISION NOT NULL)",
            ):
                conn.execute(statement)

    def upsert_sources(self, sources: list[CatalogSource], *, match_radius: float = 1.0) -> int:
        """Persist sources once, conservatively associating unique cross-catalog counterparts.

        Same-catalog neighbours remain distinct. Ambiguous matches remain separate and carry
        review metadata. Epoch propagation uses core astrometry; ids always outrank proximity.
        """
        changed = 0
        with self.transaction() as conn:
            known_catalogs = {row[0] for row in conn.execute("SELECT DISTINCT catalog FROM canonical_sources")}
            for source in sources:
                target = validate_target(source.ra, source.dec, epoch=source.epoch)
                source.ra, source.dec = target.ra, target.dec
                if not source.catalog or not source.source_id:
                    raise ValueError("Canonical sources require catalog and source identity")
                raw = source.as_dict()
                incoming_values = _values(raw)
                incoming_provenance = dict(source.provenance)
                old = conn.execute(self._sql("SELECT object_id, source_json FROM canonical_sources "
                                             "WHERE catalog=? AND source_id=?"), (source.catalog, source.source_id)).fetchone()
                if old:
                    object_id = old[0]
                    previous = json.loads(old[1])
                    if raw["metadata"].get("catalog_version") != previous.get("metadata", {}).get("catalog_version"):
                        previous = {}
                    for section in ("data", "metadata", "provenance"):
                        raw[section] = {**previous.get(section, {}), **{k: v for k, v in raw[section].items() if _present(v)}}
                    raw["metadata"]["physical"] = {**previous.get("metadata", {}).get("physical", {}),
                                                    **{k: v for k, v in source.metadata.get("physical", {}).items() if _present(v)}}
                    raw["metadata"]["first_fetched_at"] = previous.get("metadata", {}).get("first_fetched_at")
                    for key, value in previous.items():
                        if raw.get(key) is None:
                            raw[key] = value
                else:
                    object_id = _digest([source.catalog, source.source_id])[:32]
                    # Broaden declination candidates for high proper motion; exact separation below.
                    padding = max(match_radius / 3600, float(os.getenv("CANONICAL_PM_PADDING_DEG", "0.1")))
                    other_catalogs = sorted(known_catalogs - {source.catalog})
                    candidates = []
                    if other_catalogs:
                        span = min(180, padding / max(0.000001, math.cos(math.radians(min(90, abs(source.dec)+padding)))))
                        low_ra, high_ra = (source.ra-span) % 360, (source.ra+span) % 360
                        ra_condition = "" if span >= 180 else " AND " + (
                            "ra BETWEEN ? AND ?" if low_ra <= high_ra else "(ra >= ? OR ra <= ?)")
                        parameters = (*other_catalogs, source.dec-padding, source.dec+padding,
                                      *((low_ra, high_ra) if span < 180 else ()))
                        placeholders = ",".join("?" for _ in other_catalogs)
                        candidates = conn.execute(self._sql("SELECT object_id, source_json FROM canonical_sources "
                            f"WHERE catalog IN ({placeholders}) AND dec BETWEEN ? AND ?{ra_condition}"), parameters).fetchall()
                    matched = set()
                    for candidate_id, candidate_json in candidates:
                        candidate = _source(json.loads(candidate_json))
                        if angular_separation_arcsec(Target(source.ra, source.dec, epoch=source.epoch), candidate) <= match_radius:
                            matched.add(candidate_id)
                    if len(matched) == 1:
                        candidate_id = next(iter(matched))
                        same_catalog = conn.execute(self._sql("SELECT 1 FROM canonical_sources WHERE object_id=? AND catalog=?"),
                                                    (candidate_id, source.catalog)).fetchone()
                        if not same_catalog:
                            object_id = candidate_id
                    elif matched:
                        raw["metadata"]["identity_review"] = "ambiguous_position"
                raw["metadata"]["fields_present"] = sorted(k for k, v in _values(raw).items() if _present(v))
                query_key = _digest({k: v for k, v in incoming_provenance.items() if k != "retrieved_at"})
                origins = dict(raw["metadata"].get("field_provenance", {}))
                for key, value in incoming_values.items():
                    if _present(value):
                        origins[key] = {"query": query_key, "retrieved_at": incoming_provenance.get("retrieved_at")}
                queries = {**raw["metadata"].get("fetch_provenance", {}), query_key: incoming_provenance}
                referenced = {origin["query"] for origin in origins.values()}
                raw["metadata"]["field_provenance"] = origins
                raw["metadata"]["fetch_provenance"] = {key: value for key, value in queries.items() if key in referenced}
                raw["metadata"]["validation_state"] = "valid_coordinates"
                raw["metadata"]["first_fetched_at"] = raw["metadata"].get("first_fetched_at") or datetime.now(UTC).isoformat()
                encoded = json.dumps(raw, default=str)
                if not old or encoded != old[1]:
                    changed += 1
                conn.execute(self._sql("INSERT INTO canonical_sources VALUES (?, ?, ?, ?, ?, ?, ?) "
                    "ON CONFLICT(catalog, source_id) DO UPDATE SET ra=excluded.ra, dec=excluded.dec, "
                    "source_json=excluded.source_json, updated_at=excluded.updated_at"),
                    (source.catalog, source.source_id, object_id, source.ra, source.dec, encoded, datetime.now(UTC).isoformat()))
                known_catalogs.add(source.catalog)
        return changed

    def put_product(self, object_id, product, observations):
        if not observations:
            return
        with self.transaction() as conn:
            conn.execute(self._sql("INSERT INTO canonical_products VALUES (?, ?, ?, ?) "
                "ON CONFLICT(object_id, product) DO UPDATE SET product_json=excluded.product_json, updated_at=excluded.updated_at"),
                (object_id, product, json.dumps(observations, default=str), datetime.now(UTC).isoformat()))

    def iter_records(self) -> Iterator[dict[str, Any]]:
        """Stream object groups without loading the source table into memory."""
        conn = self._connect()
        try:
            cursor = conn.cursor(name="canonical_stream") if self.postgres else conn.cursor()
            cursor.execute("SELECT object_id, source_json FROM canonical_sources ORDER BY object_id, catalog, source_id")
            current = None
            members = []
            while rows := cursor.fetchmany(512):
                for object_id, raw in rows:
                    if current is not None and current != object_id:
                        yield self._record(conn, current, members)
                        members = []
                    current = object_id
                    members.append(json.loads(raw))
            if current is not None:
                yield self._record(conn, current, members)
        finally:
            conn.close()

    def _record(self, conn, object_id, members):
        products = conn.execute(self._sql("SELECT product, product_json FROM canonical_products WHERE object_id=?"),
                                (object_id,)).fetchall()
        return {"group_id": object_id, "members": members, "products": {k: json.loads(v) for k, v in products}}

    def get_record(self, object_id):
        with self._connect() as conn:
            members = conn.execute(self._sql("SELECT source_json FROM canonical_sources WHERE object_id=? ORDER BY catalog"),
                                   (object_id,)).fetchall()
            return self._record(conn, object_id, [json.loads(row[0]) for row in members])

    def claim_work(self, key, payload, owner, lease=180):
        with self.transaction() as conn:
            conn.execute(self._sql("INSERT INTO dataset_work (id,payload_json,state) VALUES (?,?,'pending') "
                                  "ON CONFLICT(id) DO NOTHING"), (key, json.dumps(payload)))
            row = conn.execute(self._sql("SELECT state, lease_until, attempts FROM dataset_work WHERE id=?"), (key,)).fetchone()
            if row[0] == "completed" or row[1] > time.time() or row[2] >= int(os.getenv("DATASET_TASK_ATTEMPTS", "3")):
                return False
            conn.execute(self._sql("UPDATE dataset_work SET state='running',owner=?,lease_until=?,attempts=attempts+1 WHERE id=?"),
                         (owner, time.time()+lease, key))
            return True

    def renew_work(self, key, owner, lease=180):
        with self._connect() as conn:
            return conn.execute(self._sql("UPDATE dataset_work SET lease_until=? WHERE id=? AND owner=? AND state='running'"),
                                (time.time()+lease, key, owner)).rowcount == 1

    def finish_work(self, key, owner, result, *, failed=False):
        with self._connect() as conn:
            changed = conn.execute(self._sql("UPDATE dataset_work SET state=?,result_json=?,lease_until=0 "
                "WHERE id=? AND owner=? AND state='running'"),
                ("failed" if failed else "completed", json.dumps(result, default=str), key, owner)).rowcount
            if not changed:
                raise RuntimeError("Work lease lost before checkpoint")

    def work(self, key):
        with self._connect() as conn:
            row = conn.execute(self._sql("SELECT state,result_json,attempts,lease_until FROM dataset_work WHERE id=?"), (key,)).fetchone()
        return {"state": row[0], "result": json.loads(row[1]) if row[1] else {}, "attempts": row[2], "lease_until": row[3]} if row else None

    def scan_state(self, key):
        with self._connect() as conn:
            row = conn.execute(self._sql("SELECT state_json,updated_at FROM archive_scans WHERE id=?"), (key,)).fetchone()
        if row and row[1] + float(os.getenv("DATASET_SCAN_TTL_SECONDS", "86400")) > time.time():
            return json.loads(row[0])
        return {}

    def advance_scan(self, key, previous_cursor, state):
        # Compare-and-swap prevents a late duplicate page from rewinding a shared frontier.
        with self.transaction() as conn:
            row = conn.execute(self._sql("SELECT state_json,updated_at FROM archive_scans WHERE id=?"), (key,)).fetchone()
            previous = json.loads(row[0]) if row and row[1] + float(os.getenv("DATASET_SCAN_TTL_SECONDS", "86400")) > time.time() else {}
            if previous.get("cursor") != previous_cursor:
                return
            conn.execute(self._sql("INSERT INTO archive_scans VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET "
                                  "state_json=excluded.state_json,updated_at=excluded.updated_at"),
                         (key, json.dumps(state), time.time()))

    def put_dataset(self, metadata: dict[str, Any]) -> None:
        with self._connect() as conn:
            conn.execute(
                self._sql(
                    "INSERT INTO datasets (id, metadata_json, created_at) VALUES (?, ?, ?) "
                    "ON CONFLICT (id) DO UPDATE SET metadata_json = excluded.metadata_json"
                ),
                (metadata["id"], json.dumps(metadata, default=str), metadata["created_at"]),
            )

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            row = conn.execute(self._sql("SELECT metadata_json FROM datasets WHERE id = ?"), (dataset_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def list_datasets(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT metadata_json FROM datasets ORDER BY created_at DESC").fetchall()
        return [json.loads(r[0]) for r in rows]

    def delete_dataset(self, dataset_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(self._sql("DELETE FROM datasets WHERE id = ?"), (dataset_id,))
            return cur.rowcount > 0

    def save_query(self, name: str, query: dict[str, Any]) -> dict[str, Any]:
        item = {
            "id": uuid.uuid4().hex,
            "name": name,
            "query": query,
            "created_at": datetime.now(UTC).isoformat(),
        }
        with self._connect() as conn:
            conn.execute(
                self._sql("INSERT INTO saved_queries (id, name, query_json, created_at) VALUES (?, ?, ?, ?)"),
                (item["id"], name, json.dumps(query), item["created_at"]),
            )
        return item

    def list_queries(self) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute("SELECT id, name, query_json, created_at FROM saved_queries ORDER BY created_at DESC").fetchall()
        return [{"id": r[0], "name": r[1], "query": json.loads(r[2]), "created_at": r[3]} for r in rows]

    def delete_query(self, query_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(self._sql("DELETE FROM saved_queries WHERE id = ?"), (query_id,))
            return cur.rowcount > 0


class ObjectStore:
    """Optional S3 / MinIO cloud storage adapter for dataset files."""

    def __init__(self, bucket: str | None = None) -> None:
        self.bucket = bucket or os.getenv("S3_BUCKET")
        self._client = None
        if self.bucket:
            try:
                import boto3
                self._client = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
            except Exception:
                self._client = None

    def put(self, dataset_id: str, path: Path) -> str | None:
        if not self._client or not self.bucket:
            return None
        key = f"datasets/{dataset_id}/{path.name}"
        self._client.upload_file(str(path), self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def get(self, uri: str):
        if not self._client or not self.bucket or not uri.startswith(f"s3://{self.bucket}/"):
            raise ValueError("Invalid dataset object URI")
        key = uri.removeprefix(f"s3://{self.bucket}/")
        return self._client.get_object(Bucket=self.bucket, Key=key)["Body"]

    def delete(self, uri: str) -> None:
        if self._client and self.bucket and uri.startswith(f"s3://{self.bucket}/"):
            self._client.delete_object(Bucket=self.bucket, Key=uri.removeprefix(f"s3://{self.bucket}/"))


# ---------------------------------------------------------------------------
# Dataset Generation Engine
# ---------------------------------------------------------------------------


@dataclass
class Coverage:
    requested_count: int
    usable_count: int
    matching_count: int
    missing_count: int
    gaps: list[dict[str, Any]]


class CoveragePlanner:
    def __init__(self, store: MetadataStore, registry: CatalogRegistry):
        self.store, self.registry = store, registry
        self._request = None

    def prepare(self, request):
        if self._request is not request:
            self._selected = request.validate_registry(self.registry)
            self._query = request.query()
            self._targets = [validate_target(t["ra"], t["dec"], epoch=t.get("epoch")) for t in request.targets]
            self._request = request

    def inspect(self, request, record):
        self.prepare(request)
        members = [m for m in record["members"] if m["catalog"] in self._selected and (
            not self.registry.get(m["catalog"]).parameters.get("version")
            or m.get("metadata", {}).get("catalog_version") == self.registry.get(m["catalog"]).parameters["version"])]
        if not members:
            return None
        query = self._query
        scores = []
        for member in members:
            if request.targets:
                sep = min(angular_separation_arcsec(t, _source(member)) for t in self._targets)
                if sep > request.radius_arcsec:
                    continue
                confidence = match_score(sep, positional_error_arcsec=member.get("positional_error_arcsec"))
            else:
                sep, confidence = 0.0, 1.0
            if confidence >= request.min_confidence:
                scores.append({**member, "separation_arcsec": sep, "confidence": confidence})
        if not scores:
            return None
        candidate = {**record, "members": scores}
        if not any(query.apply_filters(m) for m in scores):
            return None
        required = set(request.fields)
        for name, limit in {**(request.filters or {}), **request.quality_constraints}.items():
            field_name = name.removeprefix("min_").removeprefix("max_")
            value = _field_value(candidate, field_name)
            if not _present(value):
                required.add(field_name)
                continue
            if not DatasetEngine._passes_filter({field_name: value}, name, limit):
                return None
        missing = sorted(f for f in required if not _present(_field_value(candidate, f)))
        products = [p for p in request.products if not _present(record["products"].get(p))]
        return candidate, missing, products, len(scores) >= request.count_threshold

    def usable(self, request):
        for record in self.store.iter_records():
            outcome = self.inspect(request, record)
            if outcome and not outcome[1] and not outcome[2] and outcome[3]:
                yield outcome[0]

    def calculate_gap(self, request, skip_objects=()):
        usable = matching = 0
        gaps = []
        cap = int(os.getenv("DATASET_ENRICHMENT_BATCH", "128"))
        for record in self.store.iter_records():
            outcome = self.inspect(request, record)
            if outcome is None:
                continue
            candidate, missing, products, enough_members = outcome
            matching += 1
            if not missing and not products and enough_members:
                usable += 1
            elif len(gaps) < cap and record["group_id"] not in skip_objects:
                gaps.append({"object_id": record["group_id"], "fields": missing, "products": products,
                             "catalogs_present": sorted({m["catalog"] for m in candidate["members"]}),
                             "need_counterparts": not enough_members,
                             "target": {k: candidate["members"][0].get(k) for k in ("ra", "dec", "epoch")}})
        return Coverage(request.count, usable, matching, max(0, request.count-usable), gaps)


@dataclass(frozen=True)
class WorkUnit:
    dataset_id: str
    catalog: str
    provider: str
    target: dict[str, Any] | None
    radius_arcsec: float
    limit: int
    cursor: str | None = None
    object_id: str | None = None
    fields: tuple[str, ...] = ()
    product: str | None = None
    scan_key: str | None = None

    @property
    def key(self):
        return _digest(asdict(self))


class GapPlanner:
    """Enrich existing objects first; advance stable provider/target page frontiers."""
    def __init__(self, registry, store=None):
        self.registry, self.store = registry, store

    def plan(self, request, coverage, dataset_id, frontiers, attempted):
        plans = QueryBuilder(self.registry).build(request.query())
        units = []
        for gap in coverage.gaps[:coverage.missing_count]:
            by_catalog = {}
            for field_name in gap["fields"]:
                catalog, sep, column = field_name.partition(".")
                candidates = [p for p in plans if p.catalog == catalog] if sep else plans
                for plan in candidates:
                    # An unqualified field belongs to its known registry columns where possible.
                    columns = plan.parameters.get("columns", [])
                    if sep or not columns or field_name in normalize_source_record(dict.fromkeys(columns)):
                        by_catalog.setdefault(plan.catalog, []).append(column if sep else field_name)
            if gap["need_counterparts"]:
                for plan in plans:
                    if plan.catalog not in gap["catalogs_present"]:
                        by_catalog.setdefault(plan.catalog, [])
            for catalog, fields in by_catalog.items():
                definition = self.registry.get(catalog)
                unit = WorkUnit(dataset_id, catalog, provider_identity(definition.endpoint, definition.provider),
                                gap["target"], request.radius_arcsec, 100, object_id=gap["object_id"], fields=tuple(sorted(fields)))
                if unit.key not in attempted:
                    units.append(unit)
            for product in gap["products"]:
                unit = WorkUnit(dataset_id, "tess", "mast", gap["target"], min(request.radius_arcsec, 3600), 1,
                                object_id=gap["object_id"], product=product)
                if unit.key not in attempted:
                    units.append(unit)
        if units:
            return units
        # Target lists are finite frontiers. Only TAP exposes bulk keyset traversal.
        for plan in plans:
            definition = self.registry.get(plan.catalog)
            if not request.targets and definition.provider != "tap":
                continue
            for target in request.targets or [None]:
                key = _digest([plan.catalog, target])
                scan_key = _digest([definition.as_dict(), target, request.radius_arcsec])
                state = frontiers.get(key, self.store.scan_state(scan_key) if self.store else {})
                if state.get("exhausted"):
                    continue
                default_size = 1000 if plan.catalog == "gaia_dr3" else 250 if definition.provider == "tap" else 100
                size = int(definition.parameters.get("dataset_page_size", default_size))
                size = int(os.getenv(f"DATASET_{provider_identity(definition.endpoint, definition.provider).upper()}_PAGE_SIZE", size))
                unit = WorkUnit(dataset_id, plan.catalog, provider_identity(definition.endpoint, definition.provider),
                                target, request.radius_arcsec, max(1, min(size, coverage.missing_count)), cursor=state.get("cursor"),
                                scan_key=scan_key)
                if unit.key not in attempted:
                    units.append(unit)
                # Keep independent targets bounded, rather than scheduling the entire input list.
                if len(units) >= int(os.getenv("DATASET_TASK_BATCH", "16")):
                    return units
        return units


class ProviderScheduler:
    """Bounded local tasks or provider RQ queues; HTTP guards enforce shared budgets."""
    def __init__(self, engine, providers, product_fetcher=None):
        self.engine, self.providers = engine, providers
        self.product_fetcher = product_fetcher
        self.semaphores = {}

    async def _product(self, unit):
        if self.product_fetcher:
            return await self.product_fetcher(unit)
        from astroquery.mast import ObservationsClass

        from tess import TessLightCurveService
        observations = ObservationsClass()
        sessions = [observations._session, observations._portal_api_connection._session,
                    observations._service_api_connection._session]
        for session in sessions:
            session.mount("https://", budgeted_requests_adapter("mast"))
            session.mount("http://", budgeted_requests_adapter("mast"))
        service = TessLightCurveService(observations=observations)
        try:
            return await asyncio.to_thread(service.retrieve, {"ra_deg": unit.target["ra"], "dec_deg": unit.target["dec"]},
                                           radius_arcsec=unit.radius_arcsec)
        finally:
            for session in sessions:
                session.close()

    async def execute_unit(self, unit):
        store = self.engine.metadata
        owner = uuid.uuid4().hex
        deadline = time.monotonic() + float(os.getenv("DATASET_SHARD_WAIT_SECONDS", "900"))
        while not store.claim_work(unit.key, asdict(unit), owner):
            checkpoint = store.work(unit.key)
            if checkpoint and checkpoint["state"] == "completed":
                return checkpoint["result"]
            if (not checkpoint or checkpoint["attempts"] >= int(os.getenv("DATASET_TASK_ATTEMPTS", "3"))
                    or time.monotonic() >= deadline):
                return {"deferred": True, "error": "Work is leased or retry budget exhausted"}
            await asyncio.sleep(min(1, max(0.01, checkpoint["lease_until"] - time.time())))

        async def heartbeat():
            while True:
                await asyncio.sleep(30)
                if not store.renew_work(unit.key, owner):
                    raise RuntimeError("Shard lease lost")
        pulse = asyncio.create_task(heartbeat())
        try:
            if unit.product:
                result = await self._product(unit)
                observations = result.get("observations", [])
                # Never mark absent, invalid or failed products as covered.
                valid = [o for o in observations if o.get("modality") == "light_curve" and o.get("axis")
                         and len(o["axis"]) == len(o.get("values", []))
                         and any(_present(v) for v in o.get("values", []))]
                if result.get("status") == "failed":
                    raise RuntimeError("TESS retrieval failed")
                store.put_product(unit.object_id, unit.product, valid)
                outcome = {"received": len(valid), "cursor": None, "exhausted": True,
                           "failures": result.get("failures", [])}
            else:
                catalog = self.engine.registry.get(unit.catalog)
                provider = self.providers[catalog.provider]
                target = validate_target(**{k: v for k, v in unit.target.items() if k in {"ra", "dec", "epoch"}}) if unit.target else None
                cursor = None
                if hasattr(provider, "query_page"):
                    fields = list(unit.fields) or None
                    if fields:
                        # Translate normalized aliases back to registry column names.
                        known = {next(iter(normalize_source_record({c: None}))): c for c in catalog.parameters.get("columns", [])}
                        fields = [known.get(f, f) for f in fields]
                    sources, cursor = await provider.query_page(catalog, target, unit.radius_arcsec,
                                                               limit=unit.limit, cursor=unit.cursor, fields=fields)
                elif target:
                    sources = await provider.query(catalog, target, unit.radius_arcsec)
                else:
                    raise ValueError(f"{catalog.name} requires coordinate targets")
                valid = []
                invalid = 0
                for source in sources:
                    try:
                        validate_target(source.ra, source.dec, epoch=source.epoch)
                        if target and angular_separation_arcsec(target, source) > unit.radius_arcsec:
                            continue
                        valid.append(source)
                    except (InvalidCoordinateError, ValueError, TypeError, AttributeError):
                        invalid += 1
                await asyncio.to_thread(store.upsert_sources, valid)
                outcome = {"received": len(sources), "invalid": invalid, "cursor": cursor,
                           "exhausted": cursor is None or cursor == unit.cursor,
                           "query_hash": _digest([unit.catalog, unit.target, unit.cursor, unit.fields]),
                           "catalog": catalog.as_dict()}
                if unit.scan_key:
                    store.advance_scan(unit.scan_key, unit.cursor, {"cursor": cursor, "exhausted": outcome["exhausted"]})
            if pulse.done():
                pulse.result()
            store.finish_work(unit.key, owner, outcome)
            return outcome
        except Exception as exc:
            logger.exception("archive_shard_failed", task=unit.key, provider=unit.provider)
            outcome = {"error": str(exc), "error_type": type(exc).__name__}
            store.finish_work(unit.key, owner, outcome, failed=True)
            return outcome
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)

    async def execute(self, units):
        if os.getenv("DATASET_EXECUTION_MODE", "local") == "rq":
            try:
                return await self._distributed(units)
            except Exception as exc:
                logger.exception("archive_queue_unavailable", error=str(exc))
                return [{"deferred": True, "error": "Archive queue unavailable", "error_type": type(exc).__name__} for _ in units]

        async def run(unit):
            semaphore = self.semaphores.setdefault(
                unit.provider,
                asyncio.Semaphore(ProviderBudget.configured(unit.provider).max_concurrency),
            )
            async with semaphore:
                for attempt in range(int(os.getenv("DATASET_TASK_ATTEMPTS", "3"))):
                    result = await self.execute_unit(unit)
                    if not result.get("error") or result.get("deferred"):
                        return result
                    if attempt < int(os.getenv("DATASET_TASK_ATTEMPTS", "3"))-1:
                        budget = ProviderBudget.configured(unit.provider)
                        await asyncio.sleep(min(budget.backoff_max, budget.backoff_base * 2**attempt))
                return result
        return await asyncio.gather(*(run(unit) for unit in units))

    async def _distributed(self, units):
        import redis
        from rq import Queue, Retry
        from rq.exceptions import NoSuchJobError
        from rq.job import Job
        connection = redis.from_url(os.environ["REDIS_URL"], socket_connect_timeout=2, socket_timeout=2)
        results = []
        queues = {}
        for unit in units:
            queue = Queue(f"archive:{unit.provider}", connection=connection)
            queues[unit.provider] = queue
            job_id = f"shard-{unit.key}"
            try:
                job = Job.fetch(job_id, connection=connection)
                if job.is_failed:
                    results.append({"error": "RQ shard failed; checkpoint remains retryable"})
                    continue
            except NoSuchJobError:
                job = queue.enqueue(process_archive_unit, asdict(unit), str(self.engine.storage),
                                    str(self.engine.registry.registry_path) if self.engine.registry.registry_path else None,
                                    job_id=job_id, job_timeout=int(os.getenv("DATASET_SHARD_TIMEOUT", "600")),
                                    retry=Retry(max=2, interval=[10, 30]), result_ttl=86400)
            QUEUE_DEPTH.labels(unit.provider).set(len(queue))
            results.append(job)
        deadline = time.monotonic() + float(os.getenv("DATASET_SHARD_WAIT_SECONDS", "900"))
        while any(not isinstance(r, dict) for r in results) and time.monotonic() < deadline:
            for provider, queue in queues.items():
                QUEUE_DEPTH.labels(provider).set(len(queue))
            for index, result in enumerate(results):
                if isinstance(result, dict):
                    continue
                checkpoint = self.engine.metadata.work(units[index].key)
                if checkpoint and checkpoint["state"] == "completed":
                    results[index] = checkpoint["result"]
                elif result.get_status(refresh=True) in {"failed", "stopped", "canceled"}:
                    results[index] = {"error": "RQ shard failed"}
            if any(not isinstance(r, dict) for r in results):
                await asyncio.sleep(0.5)
        for provider, queue in queues.items():
            QUEUE_DEPTH.labels(provider).set(len(queue))
        return [r if isinstance(r, dict) else {"deferred": True, "error": "RQ shard wait timed out"} for r in results]


def process_archive_unit(payload, storage_path, registry_path=None):
    async def run():
        engine = DatasetEngine(storage_path=storage_path, registry_path=registry_path)
        async with httpx.AsyncClient(follow_redirects=True) as client:
            result = await ProviderScheduler(engine, provider_map(client)).execute_unit(WorkUnit(**payload))
        if result.get("error"):
            raise RuntimeError(result["error"])
        return result
    return asyncio.run(run())


@dataclass(frozen=True)
class MaterializationPolicy:
    parquet_rows: int = 10_000
    parquet_fields: int = 64
    parquet_bytes: int = 8 * 1024 * 1024
    partition_rows: int = 100_000
    partition_bytes: int = 64 * 1024 * 1024

    def __post_init__(self):
        if any(v <= 0 for v in asdict(self).values()):
            raise ValueError("Materialization thresholds must be positive")

    @classmethod
    def configured(cls):
        return cls(**{k: int(os.getenv(f"DATASET_{k.upper()}", v)) for k, v in asdict(cls()).items()})


class DatasetMaterializer:
    """Inspect a disk spool, choose a format, then stream bounded partitions."""
    def __init__(self, storage: Path, policy=None):
        self.storage = storage
        self.policy = policy or MaterializationPolicy.configured()

    def select_format(self, request, *, rows, fields, size, nested):
        if request.output_format:
            return request.output_format
        if nested:
            return "jsonl"
        if request.intended_use == "astronomy":
            return "fits"
        if rows >= self.policy.parquet_rows or fields >= self.policy.parquet_fields or size >= self.policy.parquet_bytes:
            return "parquet"
        return "csv"

    @staticmethod
    def project(record, request):
        primary = record["members"][0]
        row = {"object_id": record["group_id"], **{k: primary[k] for k in ("catalog", "source_id", "ra", "dec")}}
        if request.fields:
            row.update({f: _field_value(record, f) for f in request.fields})
        else:
            for member in record["members"]:
                row.update({f"{member['catalog']}.{k}": v for k, v in _values(member).items()
                            if k not in {"catalog", "source_id", "ra", "dec"} and _present(v)})
        for product in request.products:
            row[product] = record["products"][product]
        return row

    def materialize(self, dataset_id, request, records, manifest, reused_ids=()):
        started = time.monotonic()
        spool = self.storage / f".{dataset_id}-{uuid.uuid4().hex}.jsonl.tmp"
        written = []
        temporary = []
        try:
            kinds = {}
            count = size = 0
            reused = enriched = 0
            nested = False
            providers = {}
            with spool.open("w", encoding="utf-8") as handle:
                for record in records:
                    row = self.project(record, request)
                    for name, value in row.items():
                        kind = "nested" if isinstance(value, (dict, list)) else "bool" if isinstance(value, bool) else (
                            "int" if isinstance(value, int) and -(2**63) <= value < 2**63
                            else "float" if isinstance(value, float) else "str")
                        if value is not None:
                            kinds.setdefault(name, set()).add(kind)
                        else:
                            kinds.setdefault(name, set())
                        nested |= kind == "nested"
                    encoded = json.dumps(row, default=str, allow_nan=False) + "\n"
                    handle.write(encoded)
                    size += len(encoded.encode())
                    count += 1
                    if record["group_id"] in reused_ids:
                        reused += 1
                    elif any(m.get("metadata", {}).get("first_fetched_at", "~") < manifest.get("build_started_at", "")
                             for m in record["members"]):
                        enriched += 1
                    for member in record["members"]:
                        origin = member.get("provenance", {})
                        key = member["catalog"]
                        item = providers.setdefault(key, {"records": 0, "provider": origin.get("provider"),
                                                         "first_fetch": origin.get("retrieved_at"),
                                                         "last_fetch": origin.get("retrieved_at"),
                                                         "table": member.get("metadata", {}).get("table")})
                        item["records"] += 1
                        stamp = origin.get("retrieved_at")
                        if stamp:
                            item["first_fetch"] = min(item["first_fetch"] or stamp, stamp)
                            item["last_fetch"] = max(item["last_fetch"] or stamp, stamp)
            fmt = self.select_format(request, rows=count, fields=len(kinds), size=size, nested=nested)
            schema = pa.schema([(name, pa.bool_() if types == {"bool"} else pa.int64() if types == {"int"}
                                 else pa.float64() if types and types <= {"int", "float"} else pa.string())
                                for name, types in sorted(kinds.items())])
            if not kinds:
                schema = pa.schema([(name, pa.string()) for name in ("object_id", "catalog", "source_id", "ra", "dec")])
            destination = Path(request.export_path).resolve() if request.export_path else self.storage / f"{dataset_id}.{fmt}"
            if not destination.is_relative_to(self.storage) or destination.suffix.lower() != f".{fmt}":
                raise ValueError("export_path must be inside DATASET_STORAGE_PATH and match the selected format")
            partitioned = count > self.policy.partition_rows or size > self.policy.partition_bytes
            if destination.exists():
                raise ValueError("export_path must be unused")
            partitions = []
            with spool.open(encoding="utf-8") as handle:
                line = handle.readline()
                index = 0
                while line or index == 0:
                    index += 1
                    path = destination.with_name(f"{destination.stem}.part-{index:05d}.{fmt}") if partitioned else destination
                    if path.exists():
                        raise ValueError("Partition path must be unused")
                    stage = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
                    temporary.append(stage)
                    part_bytes = 0
                    with DatasetWriter(stage, fmt, schema=schema) as writer:
                        while line:
                            writer.write(json.loads(line))
                            part_bytes += len(line.encode())
                            line = handle.readline()
                            if writer.count >= self.policy.partition_rows or part_bytes >= self.policy.partition_bytes:
                                break
                    stage.replace(path)
                    written.append(path)
                    with path.open("rb") as binary:
                        checksum = hashlib.file_digest(binary, "sha256").hexdigest()
                    partitions.append({"path": str(path), "rows": writer.count, "bytes": path.stat().st_size, "sha256": checksum})
            manifest.update({"request": request.model_dump(), "requested_count": request.count, "returned_count": count,
                             "fields": request.fields, "products": request.products, "filters": request.filters or {},
                             "format": fmt, "providers": providers, "partitions": partitions,
                             "bytes_written": sum(p["bytes"] for p in partitions),
                             "materialization_seconds": time.monotonic()-started,
                             "format_policy": asdict(self.policy), "schema": str(schema)})
            manifest.update(reused_local_records=reused, enriched_local_records=enriched, new_records=count-reused-enriched,
                            cache_reuse_ratio=reused/max(1, count), fulfillment_status="COMPLETED" if count >= request.count else "PARTIAL")
            manifest_path = self.storage / f"{dataset_id}.manifest.json"
            stage = manifest_path.with_suffix(".json.tmp")
            temporary.append(stage)
            stage.write_text(json.dumps(manifest, indent=2, default=str), encoding="utf-8")
            stage.replace(manifest_path)
            written.append(manifest_path)
            MATERIALIZE.labels(fmt).observe(time.monotonic()-started)
            OUTPUT_BYTES.labels(fmt).inc(manifest["bytes_written"])
            return {"output_format": fmt, "total_sources": count, "partitions": partitions,
                    "manifest_path": str(manifest_path), "export_path": str(manifest_path if partitioned else destination),
                    "bytes_written": manifest["bytes_written"]}
        except BaseException:
            for path in written:
                path.unlink(missing_ok=True)
            raise
        finally:
            spool.unlink(missing_ok=True)
            for path in temporary:
                path.unlink(missing_ok=True)


class DatasetEngine:
    """Processes search targets into filtered, deduplicated datasets with durable metadata."""

    def __init__(
        self,
        registry_path: str | None = None,
        storage_path: str | None = None,
        service: CrossmatchService | None = None,
        product_fetcher=None,
    ) -> None:
        self.registry = CatalogRegistry(registry_path or os.getenv("CATALOG_REGISTRY_PATH"))
        self.storage = Path(storage_path or os.getenv("DATASET_STORAGE_PATH") or "datasets").resolve()
        self.storage.mkdir(parents=True, exist_ok=True)
        self.service = service
        self.metadata = MetadataStore(local_dir=self.storage)
        self.objects = ObjectStore()
        self.product_fetcher = product_fetcher

    async def fulfill(self, request: DatasetRequest, *, dataset_id=None):
        """Fulfill a usable object count from reusable canonical records and durable shards."""
        request.validate_registry(self.registry)
        if request.export_path:
            requested_path = Path(request.export_path).resolve()
            if not requested_path.is_relative_to(self.storage):
                raise ValueError("export_path must be inside DATASET_STORAGE_PATH")
            if request.output_format and requested_path.suffix.lower() != f".{request.output_format}":
                raise ValueError("export_path must match output_format")
        if request.count is None:
            raise ValueError("Fulfillment requires count")
        dataset_id = dataset_id or uuid.uuid4().hex
        if not dataset_id.isalnum():
            raise ValueError("Invalid dataset id")
        previous = self.metadata.get_dataset(dataset_id)
        if previous and previous.get("request_hash") not in {None, _digest(request.model_dump())}:
            raise ValueError("Dataset id already belongs to a different request")
        if previous and previous.get("status") in {"completed", "partial"}:
            if previous.get("request_hash") != _digest(request.model_dump()):
                raise ValueError("Dataset id already belongs to a different request")
            return previous
        owner = uuid.uuid4().hex
        build_key = f"build-{dataset_id}"
        if not self.metadata.claim_work(build_key, request.model_dump(), owner):
            raise RuntimeError("Dataset build is already running or retry budget exhausted")
        # Recover a crash after the manifest commit but before the metadata commit.
        manifest_path = self.storage / f"{dataset_id}.manifest.json"
        if manifest_path.is_file():
            saved = json.loads(manifest_path.read_text(encoding="utf-8"))
            if saved.get("request_hash") == _digest(request.model_dump()):
                for part in saved["partitions"]:
                    path = Path(part["path"]).resolve()
                    if not path.is_relative_to(self.storage) or not path.is_file():
                        raise RuntimeError("Recovery artifact is missing")
                    with path.open("rb") as handle:
                        if hashlib.file_digest(handle, "sha256").hexdigest() != part["sha256"]:
                            raise RuntimeError("Recovery artifact checksum mismatch")
                recovered = {**saved, "status": saved["fulfillment_status"].lower(), "phase": saved["fulfillment_status"],
                             "total_sources": saved["returned_count"], "output_format": saved["format"],
                             "catalogs_used": sorted(saved["providers"]), "manifest_path": str(manifest_path),
                             "export_uri": None, "export_path": saved["partitions"][0]["path"] if len(saved["partitions"]) == 1
                             else str(manifest_path)}
                self.metadata.put_dataset(recovered)
                self.metadata.finish_work(build_key, owner, {"dataset_id": dataset_id})
                return recovered
        metadata = {"id": dataset_id, "name": request.name, "profile": request.profile,
                    "created_at": previous["created_at"] if previous else datetime.now(UTC).isoformat(),
                    "status": "running", "total_sources": 0, "catalogs_used": [], "failures": [],
                    "request_hash": _digest(request.model_dump()), "requested_count": request.count,
                    "output_format": request.output_format, "export_uri": None}
        metadata["build_started_at"] = datetime.now(UTC).isoformat()

        def phase(value):
            metadata["phase"] = value
            self.metadata.put_dataset(metadata)
            logger.info("dataset_phase", dataset_id=dataset_id, phase=value)

        async def heartbeat():
            while True:
                await asyncio.sleep(30)
                if not self.metadata.renew_work(build_key, owner):
                    raise RuntimeError("Build lease lost")
        pulse = asyncio.create_task(heartbeat())
        try:
            phase("PLANNING")
            coverage_planner = CoveragePlanner(self.metadata, self.registry)
            gap_planner = GapPlanner(self.registry, self.metadata)
            phase("REUSING_LOCAL")
            # Keep identities, never the astronomical rows/arrays, in memory.
            reused_ids = set()
            def local_selection():
                for record in coverage_planner.usable(request):
                    reused_ids.add(record["group_id"])
                    if len(reused_ids) >= request.count:
                        break
            await asyncio.to_thread(local_selection)
            RECORDS.labels("requested").inc(request.count)
            frontiers: dict[str, Any] = {}
            attempted: set[str] = set()
            enriched: set[str] = set()
            stop_reason = "coverage_satisfied"
            query_counts: dict[str, int] = {}
            query_hashes: list[str] = []
            received = 0
            stagnant = 0
            last_coverage = None
            async with httpx.AsyncClient(follow_redirects=True) as client:
                providers = self.service.providers if self.service else provider_map(client)
                scheduler = ProviderScheduler(self, providers, self.product_fetcher)
                for round_number in range(int(os.getenv("DATASET_MAX_ROUNDS", "1000"))):
                    coverage = await asyncio.to_thread(coverage_planner.calculate_gap, request, enriched)
                    metadata["coverage"] = asdict(coverage) | {"gaps": len(coverage.gaps)}
                    if not coverage.missing_count:
                        break
                    signature = (coverage.usable_count, coverage.matching_count,
                                 sum(len(g["fields"])+len(g["products"]) for g in coverage.gaps))
                    stagnant = stagnant+1 if signature == last_coverage else 0
                    last_coverage = signature
                    if stagnant >= int(os.getenv("DATASET_NO_PROGRESS_ROUNDS", "25")):
                        stop_reason = "no_usable_progress"
                        break
                    units = gap_planner.plan(request, coverage, dataset_id, frontiers, attempted)
                    if not units:
                        stop_reason = "no_unattempted_work"
                        break
                    phase("FETCHING")
                    results = await scheduler.execute(units)
                    phase("VALIDATING")
                    for unit, result in zip(units, results):
                        attempted.add(unit.key)
                        if unit.object_id:
                            enriched.add(unit.object_id)
                        if result.get("error"):
                            metadata["failures"].append({"task": unit.key, "provider": unit.provider, **result})
                        received += result.get("received", 0)
                        query_counts[unit.provider] = query_counts.get(unit.provider, 0) + 1
                        if len(query_hashes) < 100:
                            query_hashes.append(result.get("query_hash", unit.key))
                        if not unit.object_id:
                            frontiers[_digest([unit.catalog, unit.target])] = {
                                "cursor": result.get("cursor"), "exhausted": result.get("exhausted", bool(result.get("error")))}
                    metadata["rounds"] = round_number+1
                else:
                    stop_reason = "round_limit"
            coverage = await asyncio.to_thread(coverage_planner.calculate_gap, request)
            complete = not coverage.missing_count
            metadata["coverage"] = asdict(coverage) | {"gaps": len(coverage.gaps)}
            metadata["fulfillment_status"] = "COMPLETED" if complete else "PARTIAL"
            metadata["stop_reason"] = "coverage_satisfied" if complete else stop_reason
            metadata["reused_local_records"] = len(reused_ids)
            metadata["new_records"] = min(request.count, coverage.usable_count) - len(reused_ids)
            metadata["fetched_rows"] = received
            metadata["cache_reuse_ratio"] = len(reused_ids) / max(1, min(request.count, coverage.usable_count))
            metadata["provenance"] = {"schema_version": 1, "provider_tasks": query_counts,
                                      "query_hash_sample": query_hashes, "work_checkpoint_count": len(attempted),
                                      "identity_method": "catalog/source_id; unique cross-catalog position within 1 arcsec"}
            phase("MATERIALIZING")

            def selected_records():
                count = 0
                # Preserve the initial satisfied selection in the final materialization.
                for reuse in (True, False):
                    for record in coverage_planner.usable(request):
                        if (record["group_id"] in reused_ids) != reuse:
                            continue
                        yield record
                        count += 1
                        if count >= request.count:
                            return
            manifest = {k: v for k, v in metadata.items() if k not in {"phase", "status"}}
            writing = asyncio.create_task(asyncio.to_thread(DatasetMaterializer(self.storage).materialize,
                                                            dataset_id, request, selected_records(), manifest, reused_ids))
            try:
                materialized = await asyncio.shield(writing)
            except asyncio.CancelledError:
                # Finish/clean the filesystem transaction before releasing the build lease.
                await writing
                raise
            metadata.update(materialized)
            for key in ("reused_local_records", "enriched_local_records", "new_records", "cache_reuse_ratio", "fulfillment_status"):
                metadata[key] = manifest[key]
            complete = metadata["total_sources"] >= request.count
            metadata["stop_reason"] = "coverage_satisfied" if complete else metadata["stop_reason"]
            metadata["catalogs_used"] = sorted(manifest["providers"])
            metadata["status"] = "completed" if complete else "partial"
            metadata["completed_at"] = datetime.now(UTC).isoformat()
            # Local artifacts survive S3 outages. Never discard the only usable export.
            for part in metadata["partitions"]:
                try:
                    uri = self.objects.put(dataset_id, Path(part["path"]))
                    if uri:
                        part["export_uri"] = uri
                except Exception as exc:
                    logger.exception("dataset_upload_failed", dataset_id=dataset_id)
                    metadata["failures"].append({"storage": "s3", "error": str(exc)})
            phase(metadata["fulfillment_status"])
            if pulse.done():
                pulse.result()
            self.metadata.finish_work(build_key, owner, {"dataset_id": dataset_id})
            BUILDS.labels(metadata["status"]).inc()
            RECORDS.labels("reused").inc(metadata["reused_local_records"])
            RECORDS.labels("new").inc(metadata["new_records"])
            RECORDS.labels("enriched").inc(metadata["enriched_local_records"])
            REUSE.observe(metadata["cache_reuse_ratio"])
            return metadata
        except BaseException as exc:
            metadata.update(status="failed", phase="FAILED", error=str(exc))
            self.metadata.put_dataset(metadata)
            self.metadata.finish_work(build_key, owner, {"error": str(exc)}, failed=True)
            BUILDS.labels("failed").inc()
            raise
        finally:
            pulse.cancel()
            await asyncio.gather(pulse, return_exceptions=True)

    async def create_dataset(
        self,
        name: str,
        profile: str,
        radius_arcsec: float,
        object_types: list[str] | None = None,
        count_threshold: int = 1,
        time_period: dict[str, Any] | None = None,
        catalogs: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        output_format: str | None = None,
        export_path: str | None = None,
        targets: list[dict[str, Any]] | None = None,
        min_confidence: float = 0.0,
        max_results: int | None = None,
        dataset_id: str | None = None,
        count: int | None = None,
        fields: list[str] | None = None,
        products: list[str] | None = None,
        quality_constraints: dict[str, Any] | None = None,
        intended_use: str = "table",
    ) -> dict[str, Any]:
        """Execute crossmatches across targets and stream filtered detections into an export file."""
        if count is not None:
            request = DatasetRequest.model_validate({
                "name": name,
                "profile": profile,
                "radius_arcsec": radius_arcsec,
                "count": count,
                "fields": fields or [],
                "products": products or [],
                "quality_constraints": quality_constraints or {},
                "intended_use": intended_use,
                "object_types": object_types,
                "count_threshold": count_threshold,
                "time_period": time_period,
                "catalogs": catalogs,
                "filters": filters,
                "output_format": output_format,
                "export_path": export_path,
                "targets": targets or [],
                "min_confidence": min_confidence,
                "max_results": max_results,
            })
            return await self.fulfill(request, dataset_id=dataset_id)
        if fields or products or quality_constraints:
            raise ValueError("fields/products/quality_constraints require count")
        output_format = output_format or "parquet"
        if not name.strip() or not profile.strip():
            raise ValueError("name and profile are required")
        if output_format.lower() not in {"json", "jsonl", "csv", "parquet", "fits"}:
            raise ValueError("Unsupported output format")
        if not targets:
            raise ValueError("At least one target with ra and dec is required")

        unknown = set(catalogs or ()) - set(self.registry.enabled_catalogs())
        if unknown:
            raise ValueError(f"Unknown catalogs: {', '.join(sorted(unknown))}")

        query = AdvancedQuery.from_dict({
            "ra": targets[0]["ra"],
            "dec": targets[0]["dec"],
            "radius_arcsec": radius_arcsec,
            "object_types": object_types,
            "count_threshold": count_threshold,
            "min_confidence": min_confidence,
            "time_period": time_period,
            "catalogs": catalogs,
            "filters": filters,
            "max_results": max_results,
            "profiles": [profile],
        })
        QueryValidator.validate(query, self.registry)

        for target in targets:
            validate_target(target["ra"], target["dec"], epoch=target.get("epoch"))

        dataset_id = dataset_id or uuid.uuid4().hex
        path = Path(export_path).resolve() if export_path else self.storage / f"{dataset_id}.{output_format.lower()}"
        if not path.is_relative_to(self.storage):
            raise ValueError("output_path must be within DATASET_STORAGE_PATH")
        if path.exists() or path.suffix.lower() != f".{output_format.lower()}":
            raise ValueError("output_path must be unused and match output_format")

        seen: set[tuple[str, str]] = set()
        catalogs_used: set[str] = set()
        failures: list[dict[str, Any]] = []

        async def collect(active_service: CrossmatchService, writer: DatasetWriter) -> None:
            async for result in self._search_targets(active_service, targets, query):
                failures.extend(result.get("failures", []))
                for group in result["crossmatch_groups"]:
                    self.metadata.upsert_sources([_source(s) for s in group["members"]])
                    members = [
                        source for source in group["members"]
                        if source["confidence"] >= min_confidence
                        and query.apply_filters(source)
                        and (not catalogs or source["catalog"] in catalogs)
                        and all(self._passes_filter(source, k, v) for k, v in (filters or {}).items())
                    ]
                    if len(members) < count_threshold:
                        continue
                    for source in members:
                        key = (source["catalog"], source["source_id"])
                        if key in seen:
                            continue
                        seen.add(key)
                        catalogs_used.add(source["catalog"])
                        writer.write(source)

        with DatasetWriter(path, output_format) as writer:
            if self.service is None:
                async with httpx.AsyncClient(follow_redirects=True) as client:
                    svc = CrossmatchService(self.registry, provider_map(client))
                    await collect(svc, writer)
            else:
                await collect(self.service, writer)

        try:
            export_uri = self.objects.put(dataset_id, path)
        except Exception as exc:
            logger.exception("dataset_upload_failed", dataset_id=dataset_id)
            failures.append({"storage": "s3", "error": str(exc)})
            export_uri = None
        if export_uri:
            path.unlink()

        previous = self.metadata.get_dataset(dataset_id)
        metadata = {
            "id": dataset_id,
            "name": name,
            "profile": profile,
            "status": "completed",
            "total_sources": writer.count,
            "catalogs_used": sorted(catalogs_used),
            "failures": failures,
            "output_format": output_format.lower(),
            "export_path": str(path),
            "created_at": previous["created_at"] if previous else datetime.now(UTC).isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "export_uri": export_uri,
        }
        manifest_path = self.storage / f"{dataset_id}.manifest.json"
        manifest_path.write_text(json.dumps({**metadata, "request": query.to_dict(), "targets": targets,
                                            "requested_count": None, "returned_count": writer.count,
                                            "format": output_format, "provenance": {"mode": "legacy_detections"}},
                                           default=str, indent=2), encoding="utf-8")
        metadata["manifest_path"] = str(manifest_path)
        self.metadata.put_dataset(metadata)
        return metadata

    @staticmethod
    async def _search_targets(service: CrossmatchService, targets: list[dict[str, Any]], base_query: AdvancedQuery):
        semaphore = asyncio.Semaphore(10)

        async def search(t: dict[str, Any]) -> dict[str, Any]:
            async with semaphore:
                q_dict = base_query.to_dict()
                q_dict["target"] = t
                q = AdvancedQuery.from_dict(q_dict)
                result = await service.crossmatch(t["ra"], t["dec"], query=q)
                return result.as_dict()

        for offset in range(0, len(targets), 10):
            batch = targets[offset:offset + 10]
            for res in await asyncio.gather(*(search(t) for t in batch)):
                yield res

    @staticmethod
    def _passes_filter(source: dict[str, Any], name: str, limit: Any) -> bool:
        field = name.removeprefix("min_").removeprefix("max_")
        val = source.get(field)
        if val is None:
            val = source.get("physical", {}).get(field, source.get("data", {}).get(field))
        if val is None and field == "proper_motion_masyr":
            ra = source.get("proper_motion_ra_masyr")
            dec = source.get("proper_motion_dec_masyr")
            if ra is not None and dec is not None:
                try:
                    val = math.hypot(float(ra), float(dec))
                except (TypeError, ValueError):
                    return False
        if val is None:
            return False
        try:
            if name.startswith("min_"):
                return float(val) >= float(limit)
            if name.startswith("max_"):
                return float(val) <= float(limit)
        except (TypeError, ValueError):
            return False
        return str(val) == str(limit)

    def list_datasets(self) -> list[dict[str, Any]]:
        return self.metadata.list_datasets()

    def get_dataset(self, dataset_id: str) -> dict[str, Any] | None:
        if not dataset_id.isalnum():
            return None
        return self.metadata.get_dataset(dataset_id)

    def delete_dataset(self, dataset_id: str) -> bool:
        metadata = self.get_dataset(dataset_id)
        if metadata is None:
            return False
        if metadata.get("status") in {"queued", "running"}:
            raise ValueError("A running dataset cannot be deleted")
        if metadata.get("export_uri"):
            self.objects.delete(metadata["export_uri"])
        if metadata.get("export_path"):
            p = Path(metadata["export_path"]).resolve()
            if p.is_relative_to(self.storage):
                p.unlink(missing_ok=True)
        for part in metadata.get("partitions", []):
            p = Path(part["path"]).resolve()
            if p.is_relative_to(self.storage):
                p.unlink(missing_ok=True)
            if part.get("export_uri"):
                self.objects.delete(part["export_uri"])
        if metadata.get("manifest_path"):
            p = Path(metadata["manifest_path"]).resolve()
            if p.is_relative_to(self.storage):
                p.unlink(missing_ok=True)
        return self.metadata.delete_dataset(dataset_id)


# ---------------------------------------------------------------------------
# Asynchronous Jobs and Queue Submission
# ---------------------------------------------------------------------------


def enqueue_dataset(payload: dict[str, Any], engine: DatasetEngine | None = None) -> dict[str, Any]:
    """Register a new dataset in the metadata store with status 'queued'."""
    eng = engine or DatasetEngine()
    dataset_id = uuid.uuid4().hex
    metadata = {
        "id": dataset_id,
        "name": payload["name"],
        "profile": payload["profile"],
        "status": "queued",
        "total_sources": 0,
        "catalogs_used": [],
        "failures": [],
        "output_format": payload["output_format"],
        "export_path": None,
        "export_uri": None,
        "created_at": datetime.now(UTC).isoformat(),
    }
    eng.metadata.put_dataset(metadata)
    return metadata


async def process_dataset_async(dataset_id: str, payload: dict[str, Any], engine: DatasetEngine | None = None) -> None:
    """Execute dataset generation asynchronously, updating job status."""
    engine = engine or DatasetEngine()
    metadata = engine.get_dataset(dataset_id)
    if metadata is None:
        return
    if metadata.get("status") in {"completed", "partial"}:
        return
    if payload.get("count") is not None:
        await engine.create_dataset(**payload, dataset_id=dataset_id)
        return
    metadata["status"] = "running"
    engine.metadata.put_dataset(metadata)
    logger.info("dataset_started", dataset_id=dataset_id)
    try:
        await engine.create_dataset(**payload, dataset_id=dataset_id)
        logger.info("dataset_completed", dataset_id=dataset_id)
    except Exception as exc:
        metadata["status"] = "failed"
        metadata["error"] = str(exc)
        engine.metadata.put_dataset(metadata)
        logger.error("dataset_failed", dataset_id=dataset_id, error=str(exc))
        raise


def process_dataset(dataset_id: str, payload: dict[str, Any]) -> None:
    """Sync wrapper for worker executors."""
    asyncio.run(process_dataset_async(dataset_id, payload))


def submit_to_redis(dataset_id: str, payload: dict[str, Any]) -> bool:
    """Attempt enqueueing to Redis RQ; return False if Redis is not configured."""
    url = os.getenv("REDIS_URL")
    if not url:
        return False
    try:
        import redis
        from rq import Queue

        queue = Queue("datasets", connection=redis.from_url(url, socket_connect_timeout=2, socket_timeout=2))
        queue.enqueue(process_dataset, dataset_id, payload, job_id=dataset_id, job_timeout=3600)
        return True
    except Exception as exc:
        logger.warning("redis_enqueue_failed", error=str(exc))
        return False
