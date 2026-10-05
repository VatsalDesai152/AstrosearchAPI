# AstroSearch codebase guide

AstroSearch's runtime is organized into seven Python modules. The old standalone model/provider, astronomy pipeline, signal pipeline, and TESS adapter modules have been folded into these files. Tests, documentation, packaging configuration, and runtime data are separate support assets; they are not extra runtime modules.

## Runtime modules

| Module | Responsibility | Main interfaces |
|---|---|---|
| [`core.py`](core.py) | Coordinate and source models, catalog registry, archive parsers and providers, name resolution, query validation/planning, proper-motion handling, scoring, and crossmatching. | `CatalogRegistry`, `AdvancedQuery`, `CrossmatchService`, provider adapters |
| [`datasets.py`](datasets.py) | Dataset request validation, canonical record reuse, provider work planning, concurrent fulfillment, streaming exports, metadata, local/S3 storage, and optional Redis/RQ jobs. | `DatasetRequest`, `DatasetEngine`, `MetadataStore` |
| [`api.py`](api.py) | FastAPI application, request schemas, routes, CORS, authentication, quotas, request metrics, and service lifecycle. | `app`, `create_app()` |
| [`main.py`](main.py) | Public Python facade, configured service construction, crossmatch helpers, central CLI, and offline verification command. | `build_service()`, `crossmatch()`, `search_object()`, `main()` |
| [`astronomy.py`](astronomy.py) | Object and exoplanet-system summaries, bounded archive snapshots, SIMBAD host matching, Mantis export/publication/refresh, and catalog plots. | `object_summary()`, `summarize_system()`, CLI commands |
| [`signals.py`](signals.py) | Canonical time-series/spectrum representations, reference matching and triage, immutable delivery snapshots, Mantis signal exports, and plots. | `represent_signal()`, `cross_reference_observation()`, `commit_signal_batch()` |
| [`tess.py`](tess.py) | TESS SPOC FITS conversion, MAST retrieval/cache, canonical observations, JSONL output, and light-curve plotting. | `read_tess_light_curve()`, `TessLightCurveService`, CLI commands |

`pyproject.toml` lists exactly these seven modules for installation. The four files under `tests/` are test suites, not application scripts.

## Main data paths

### Catalog search

The API or Python facade accepts sky coordinates or an object name. Names are resolved through CDS Sesame, then the query planner selects catalog/provider adapters from the configured registry. Provider rows are normalized, scored, grouped into candidate counterparts, and returned with failures and provenance so partial archive responses remain visible.

The search path supports cone, shell, or cylinder modes, catalog/profile selection, physical filters, proper-motion handling, confidence thresholds, and batch searches. Catalog detections and scalar measurements are returned; the service does not retrieve general image cutouts or spectra.

### Dataset generation

Dataset creation can use an explicit target list or a requested usable-object count. Count-based fulfillment checks durable canonical records first, plans missing fields/products, fetches provider work, deduplicates and validates results, and materializes JSON/JSONL, CSV, Parquet, or FITS with a provenance manifest. API clients submit a job, poll its status, and download the completed or partial artifact.

SQLite is the default metadata store. PostgreSQL, S3/MinIO, and Redis/RQ are optional integrations. The `datasets/` directory is a default runtime storage location; files there can be generated user data and should not be treated as disposable build output.

### Astronomy summaries

Object summaries interpret crossmatch evidence. System summaries use bounded Exoplanet Archive data and SIMBAD host identity matching. Separate CLI workflows synchronize snapshot state and publish/watch Mantis maps. The Mantis extension directory in this checkout is a manifest and README scaffold, not a built UI panel.

### Signals and TESS

Signal processing accepts externally supplied canonical light curves or spectra and makes a versioned 48-value representation. Cross-reference results report evidence and review status; they do not claim a discovery. Bulk delivery snapshot commits remain available through the signal CLI.

The TESS adapter retrieves public SPOC light curves from MAST, caches products, preserves flux uncertainties, quality flags, and provenance, and emits canonical observations. It supports `SAP_FLUX` and `PDCSAP_FLUX`. This is the integrated archive-media retrieval path; general spectrum or image retrieval is not implemented.

## HTTP API for a UI

Start the service with:

```sh
python main.py serve --host 127.0.0.1 --port 8000
```

The OpenAPI UI is at `http://127.0.0.1:8000/api/docs`; its JSON schema is at `/api/openapi.json`.

| UI operation | Endpoint | Input/output |
|---|---|---|
| Health and catalog discovery | `GET /api/v1/health`, `GET /api/v1/catalogs`, `GET /api/v1/catalogs/{name}` | JSON |
| Single and batch crossmatch | `POST /api/v1/search`, `POST /api/v1/search/batch` | Search JSON in; evidence and provenance JSON out |
| Object/system summaries | `POST /api/v1/summaries/object`, `POST /api/v1/summaries/system` | JSON in; summary JSON out |
| Saved searches | `GET/POST /api/v1/queries`, `GET /api/v1/queries/{id}`, `POST /api/v1/queries/{id}/run`, `DELETE /api/v1/queries/{id}` | Save, inspect, rerun, or remove a search |
| Dataset jobs | `POST /api/v1/datasets/create`, `GET /api/v1/datasets`, `GET /api/v1/datasets/{id}`, `GET /api/v1/datasets/{id}/export`, `GET /api/v1/datasets/{id}/manifest`, `DELETE /api/v1/datasets/{id}` | JSON job request; `202` with `Location`; poll and download the artifact |
| Signal cross-reference | `POST /api/v1/signals/cross-reference` | Canonical observation and references in; representation and triage evidence out |
| TESS light curves | `POST /api/v1/signals/tess/light-curves`, `POST /api/v1/signals/tess/light-curves/plot` | Canonical observation JSON or PNG bytes |
| Diagnostics | `GET /api/v1/stats`, `GET /api/v1/monitoring`, `GET /api/v1/monitoring/metrics` | JSON or Prometheus text |

Search inputs must provide either a nonblank `name` or both `ra` and `dec`. Validation errors use `422`. Configure `CORS_ORIGINS` for browser origins. If API keys or JWTs are enabled, the actual UI request must authenticate; browser preflight requests are allowed to reach CORS handling. Do not embed privileged keys in public frontend code; use a trusted proxy for production authentication.

Catalog source plots from local files, Mantis publication/refresh, local FITS plotting, and bulk signal snapshot creation remain operator workflows. Their implementation stays in the seven runtime modules and their existing commands; not every file-oriented or external-publication operation is exposed as an HTTP route.

For complete request fields and response examples, see [API and dataset documentation](DOCUMENTATION.md), [frontend integration](FRONTEND_INTEGRATION.md), [astronomy workflow](ASTRONOMY.md), and [signal representation guide](SIGNAL_REPRESENTATIONS.md).

## Installed commands

| Command | Purpose |
|---|---|
| `astrosearch` | Serve the API; search; build datasets; list catalogs; benchmark exports; run offline verification. |
| `astrosearch-astronomy` | Summarize objects/systems; synchronize and publish bounded astronomy snapshots; monitor for changes. |
| `astrosearch-signals` | Read telescope-delivery JSONL, cross-reference observations, and commit immutable results. |
| `astrosearch-plot-catalogs` | Plot catalog positions and available scalar measurements from a query or saved export. |
| `astrosearch-tess` | Retrieve TESS SPOC products or plot local FITS light curves. |

Install the project and development dependencies with `pip install -e '.[dev]'`. Python 3.12 or newer is required. The runtime module list and command entry points are maintained in [`pyproject.toml`](pyproject.toml).

## Repository support files

- `tests/` contains unit and API tests and is retained independently from the seven runtime modules.
- `*.md` files document API contracts and specialized scientific/operator workflows.
- `.env.example` lists environment configuration; copy to a local `.env` and provide deployment-specific values there.
- `metadata.sqlite3` and the dataset storage directory can contain persistent application state. Preserve them during source cleanup unless their data is intentionally being reset.
- `.mypy_cache/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`, and `astrosearch.egg-info/` are generated tooling or packaging artifacts and can be recreated.
