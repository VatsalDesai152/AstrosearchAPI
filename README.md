# AstroSearch

See the [codebase guide](CODEBASE_GUIDE.md) for the seven-module architecture, UI API map, workflows, retained support files, and cleanup boundaries.

## Astronomical Summarizer and MIT CSAIL Mantis

Object and extrasolar-system summaries, bounded Exoplanet Archive snapshots, SIMBAD host identity cross-references, typed Mantis map exports, and recurring refresh support are documented in [ASTRONOMY.md](ASTRONOMY.md).

Time-series and spectral ingestion, versioned signal vectors, Mantis similarity maps, and scientifically bounded novelty triage are documented in [SIGNAL_REPRESENTATIONS.md](SIGNAL_REPRESENTATIONS.md).

The `mantis-extension/astrosearch-observatory` directory is currently a manifest/README scaffold. It has no panel source or built panel bundle; see the backend API documentation before implementing a frontend.

Frontend developers can start with the [UI/API integration guide](FRONTEND_INTEGRATION.md) and use the [technical media/API contracts](DOCUMENTATION.md#media-plotting--frontend-integration) for detailed request and response behavior.

```sh
pip install -e '.[dev]'
python -m astronomy summarize TRAPPIST-1
python -m astronomy sync
python -m astronomy publish
python -m signals telescope-delivery.jsonl --references known-signal-references.json
astrosearch-plot-catalogs --name M87 --radius 30 --output-dir plots/m87
astrosearch-tess retrieve --tic-id 141914082 --sector 1 --jsonl data/tic-141914082.jsonl --plot-dir plots/tic-141914082
```

`astrosearch-plot-catalogs` queries the infrared and radio catalog profiles and writes source-position and available catalog measurement plots. Use `--input result.json` (or a CSV, Parquet, or FITS dataset export) to plot saved results without querying archives. TESS SPOC light curves can be retrieved from MAST, cached, returned in the canonical signal format, plotted, or written as JSONL for the signal pipeline. The [technical documentation](DOCUMENTATION.md#media-plotting--frontend-integration) describes supported media, API contracts, cache behavior, and current limitations.

The installed commands remain `astrosearch`, `astrosearch-astronomy`, `astrosearch-signals`, `astrosearch-plot-catalogs`, and `astrosearch-tess`. The corresponding modules are now grouped under the seven-file runtime layout below; use `core`, `signals`, and `tess` for direct imports.

Mantis publication requires a valid local `mantis setup` connection. The summary and data pipeline work independently of Mantis authentication.

**AstroSearch** is a high-performance Python backend system for cross-matching sky coordinates and astronomical object identities across major public astronomical survey archives (Gaia, SIMBAD, NED, 2MASS, AllWISE, Pan-STARRS, SDSS, FIRST, NVSS, Chandra, XMM, etc.), applying astrophysical filters, and generating streaming datasets in JSON, CSV, Parquet, and FITS formats.

The backend runtime is consolidated into seven Python modules. Tests remain under `tests/`.

Dataset requests with `count` use durable canonical records before querying archives. They plan
missing fields/products, run independent provider tasks, deduplicate and recheck usable coverage,
then select CSV, Parquet, FITS or JSONL and write a provenance manifest. Requests without `count`
retain the existing target/detection export behavior. For example:

```sh
astrosearch dataset --name nearby-stars --profile stellar --catalogs gaia_dr3 --count 100000 --fields ra dec parallax --filters '{"min_parallax": 1}' --format auto
```

See [dataset fulfillment](DOCUMENTATION.md#7-building-larger-datasets) for the API, exact format
thresholds, provider budgets, RQ worker setup, persistence schema, and operating limits.

---

## Architecture

```
AstroSearch/
|-- core.py      # Models, parsers, providers, query DSL, astrometry, crossmatching
|-- datasets.py  # Streaming exports, metadata/storage, and worker jobs
|-- api.py       # FastAPI service, routes, auth, quotas, and metrics
|-- main.py      # Programmatic facade, search CLI, and offline verification
|-- astronomy.py # Evidence summaries, snapshots, Mantis publication, catalog plots
|-- signals.py   # Signal vectors, cross-reference, delivery pipeline, plots
`-- tess.py      # TESS FITS adapter, MAST retrieval/cache, and CLI
```

1. **[core.py](core.py)**: Data models, parsing, 16 catalog definitions (15 enabled), archive adapters, name resolver/cache, query validation and planning, proper-motion propagation, scoring, and counterpart grouping.
2. **[datasets.py](datasets.py)**: Streaming JSON/CSV/Parquet/FITS output, dataset execution, SQLite/PostgreSQL metadata, S3/MinIO storage, and async jobs.
3. **[api.py](api.py)**: Versioned REST routes for health, catalogs, search, datasets, summaries, signals, TESS, saved queries, and diagnostics, with auth, quotas, and metrics.
4. **[main.py](main.py)**: Programmatic facade and `astrosearch` CLI (`serve`, `search`, `dataset`, `catalogs`, `benchmark`, `verify`).
5. **[astronomy.py](astronomy.py)**: Evidence-based object/system summaries, bounded Exoplanet Archive snapshots, SIMBAD identity matching, Mantis export/publication/refresh, and catalog source plots.
6. **[signals.py](signals.py)**: Fixed 48-value signal representations, evidence-bounded reference matching/triage, immutable telescope deliveries, Mantis signal exports, and quality-aware plots.
7. **[tess.py](tess.py)**: TESS SPOC FITS conversion, MAST retrieval and cache, canonical observation output, JSONL export, and local/retrieved plotting CLI.
---

## 🚀 Installation

Requires **Python 3.12+**.

```bash
# Clone and enter workspace
git clone https://github.com/FungousLand1941/AstrosearchAPI.git
cd AstrosearchAPI

# Create virtual environment
python -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\Activate.ps1

# Install dependencies
pip install -e '.[dev]'
```

### Key Dependencies
- `astropy>=6.0.0` (Coordinate frames, astrometric transformations, IPAC tables, FITS)
- `fastapi>=0.110.0` & `uvicorn>=0.29.0` (REST API service)
- `pydantic>=2.7.0` (Data validation and schema contracts)
- `httpx>=0.27.0` (Async HTTP archive client)
- `astroquery>=0.4.11` (MAST observation and product queries)
- `pyarrow>=15.0.0` (Streaming Parquet export with zstd compression)
- `structlog` & `prometheus-client` (Structured observability and metrics)
- Optional: `redis`, `rq`, `psycopg`, `boto3` (Distributed workers, PostgreSQL, and S3 storage)

---

## 💻 Python Library Quickstart

### 1. Crossmatch by Coordinates
```python
import asyncio
from main import crossmatch

async def main():
    # Crossmatch near Virgo A (M87) with a 5.0 arcsecond radius
    result = await crossmatch(187.705930, 12.391123, radius_arcsec=5.0)
    print(f"Catalogs queried: {result.catalogs_queried}")
    print(f"Physical objects clustered: {len(result.crossmatch_groups)}")
    for wavelength, sources in result.counterparts.items():
        print(f"  [{wavelength.upper()}] {len(sources)} detection(s)")

asyncio.run(main())
```

### 2. Crossmatch by Astronomical Object Name (CDS Sesame)
```python
import asyncio
from main import search_object

async def main():
    # Resolves object name to canonical coordinates, then crossmatches
    result = await search_object("M87", radius_arcsec=5.0, profile="optical")
    print("Resolved Name:", result.resolved_object["canonical_name"])
    print("Coordinates:", result.target["ra"], result.target["dec"])
    print("Matches:", len(result.provenance["matches"]))

asyncio.run(main())
```

### 3. Advanced Query with Physical and Spatial Filters
```python
import asyncio
from core import AdvancedQuery, CrossmatchService
from main import build_service

async def main():
    service = build_service()
    query = AdvancedQuery.from_dict({
        "ra": 187.705930,
        "dec": 12.391123,
        "radius_arcsec": 15.0,
        "object_types": ["galaxy", "quasar"],
        "min_confidence": 0.8,
        "search_mode": "cone",
        "proper_motion": True,
        "adaptive_radius": True,
    })
    result = await service.crossmatch(187.705930, 12.391123, query=query)
    print("Effective Radius:", result.provenance["effective_radius_arcsec"])

asyncio.run(main())
```

---

## 🛠️ Command-Line Interface (CLI)

`main.py` provides a unified operational command-line interface:

### Start the REST API Server
```bash
python main.py serve --host 127.0.0.1 --port 8000
```
API Documentation will be live at `http://127.0.0.1:8000/api/docs`.

### Search by Sky Position
```bash
python main.py search --ra 187.27792 --dec 2.05239 --radius 3.0 --profile optical
```

### Search by Object Name
```bash
python main.py search --name "M87" --radius 5.0 --format json
```

### Inspect Available Catalogs
```bash
python main.py catalogs
python main.py catalogs --name gaia_dr3
```

### Export Benchmark
```bash
python main.py benchmark --rows 50000 --format parquet
```

### Run Built-in Offline Verification Suite
```bash
python main.py verify
```

### Retrieve, Plot, and Export TESS Light Curves
```bash
astrosearch-tess retrieve --tic-id 141914082 --sector 1 --flux-kind PDCSAP_FLUX \
  --jsonl data/tic-141914082.jsonl --plot-dir plots/tic-141914082
astrosearch-tess plot sector-1_lc.fits --output-dir plots/local --show-uncertainties
```
The retrieval command uses a persistent product cache and writes one canonical JSONL observation per product. See [TESS API and frontend integration](DOCUMENTATION.md#tess-api-contract) for coordinate/name targets, options, payloads, cache behavior, and response statuses.

---

## 🌐 HTTP REST API

The FastAPI service in `api.py` exposes the full REST API:

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/v1/health` | Service health status and timestamp |
| `GET` | `/api/v1/catalogs` | List 16 catalog definitions (15 enabled by default) |
| `GET` | `/api/v1/catalogs/{name}` | Retrieve catalog parameters and table info |
| `POST` | `/api/v1/search` | Single coordinate or object name crossmatch |
| `POST` | `/api/v1/search/batch` | Concurrency-limited batch crossmatch |
| `POST` | `/api/v1/summaries/object` | Summarize crossmatch evidence for an object |
| `POST` | `/api/v1/summaries/system` | Summarize an extrasolar system |
| `POST` | `/api/v1/signals/cross-reference` | Represent and compare a supplied light curve or spectrum |
| `POST` | `/api/v1/datasets/create` | Submit asynchronous dataset creation (HTTP 202) |
| `GET` | `/api/v1/datasets` | List all created datasets |
| `GET` | `/api/v1/datasets/{id}` | Inspect dataset generation status & metadata |
| `GET` | `/api/v1/datasets/{id}/export` | Download dataset (JSON, CSV, Parquet, FITS) |
| `DELETE` | `/api/v1/datasets/{id}` | Delete dataset and local/S3 artifacts |
| `GET` | `/api/v1/queries` | List saved queries |
| `GET` | `/api/v1/queries/{id}` | Retrieve one saved query |
| `POST` | `/api/v1/queries` | Save query definition |
| `POST` | `/api/v1/queries/{id}/run` | Run a saved query and return crossmatch JSON |
| `DELETE` | `/api/v1/queries/{id}` | Delete saved query |
| `GET` | `/api/v1/stats` | System metrics (datasets, exported sources) |
| `GET` | `/api/v1/monitoring` | Provider circuit breaker states |
| `GET` | `/api/v1/monitoring/metrics` | Prometheus scrape endpoint |
| `POST` | `/api/v1/signals/tess/light-curves` | Retrieve canonical TESS SPOC observations by TIC, name, or coordinates |
| `POST` | `/api/v1/signals/tess/light-curves/plot` | Retrieve TESS SPOC observations and return a PNG plot |

For detailed API payload specifications, see [DOCUMENTATION.md](DOCUMENTATION.md).

---

## License

MIT. See [LICENSE](LICENSE).
