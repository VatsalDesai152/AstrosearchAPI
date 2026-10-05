# AstroSearch capability audit

See [CODEBASE_GUIDE.md](CODEBASE_GUIDE.md) for the definitive seven-module architecture and UI/API map. This file focuses on the capability and consolidation audit.

## Current shape

The runtime now has seven Python modules. Five installed commands expose the operator workflows; tests remain in `tests/` and are not runtime scripts.

| Runtime module | Consolidated responsibilities |
|---|---|
| `core.py` | Models, catalog registry, parsers, archive providers, name resolution, caching, query planning, astrometry, and crossmatching. |
| `datasets.py` | Multi-target generation, JSON/CSV/Parquet/FITS streaming, metadata/storage, and async jobs. |
| `api.py` | REST routes, request schemas, auth, quotas, metrics, and service lifecycle. |
| `main.py` | Programmatic facade and central search/dataset/catalog CLI. |
| `astronomy.py` | Object/system summaries, bounded archive snapshots, SIMBAD identity matching, Mantis export/publication/refresh, catalog plots. |
| `signals.py` | Canonical time-series/spectrum vectors, reference cross-reference and triage, immutable deliveries, Mantis signal export, signal plots. |
| `tess.py` | TESS SPOC FITS conversion, MAST retrieval/cache, canonical observations, JSONL, and TESS CLI. |

## Capabilities present in the code

| Area | Current capabilities |
|---|---|
| Catalog setup and parsing | 16 catalog definitions (15 enabled by default); coordinate and source normalization; VOTable, IPAC, CSV, and JSON parsing. |
| Archive access | Async adapters for TAP/ADQL, IRSA Gator, MAST, SDSS, and HEASARC Xamin; CDS Sesame name resolution; cache, rate limiting, and circuit-breaker support. |
| Crossmatching | Query validation/building, sky-coordinate and proper-motion handling, probabilistic scoring, spatial/physical filters, and counterpart grouping. |
| Datasets | Multi-target generation and streaming JSON, CSV, Parquet, and FITS output; metadata/storage abstractions; async jobs and optional Redis/RQ, PostgreSQL, and S3 integrations. |
| HTTP API | Versioned routes for health/catalog/search/batch-search; datasets and saved-query lifecycle; astronomy summaries; signal cross-reference; TESS retrieval/plotting; stats, monitoring, authentication, quotas, and Prometheus metrics. |
| Astronomy | Object/system summaries; bounded Exoplanet Archive snapshots with SIMBAD identity matching; change tracking; Mantis map export/publication and recurring refresh. |
| Signals | Canonical time-series/spectrum observations; deterministic 48-value representations; catalog/reference cross-reference; immutable delivery snapshots and Mantis export; review triage. |
| TESS and plotting | MAST TESS SPOC light-curve retrieval and cache; FITS-to-canonical conversion; JSONL export; local/retrieved light-curve plots; catalog source plots from live queries or saved JSON/CSV/Parquet/FITS. |

## Installed commands

| Command | Entry module | Main use |
|---|---|---|
| `astrosearch` | `main.py` | Serve API, search, build datasets, inspect catalogs, benchmark, run offline verification. |
| `astrosearch-astronomy` | `astronomy.py` | Summarize objects/systems, sync snapshots, publish, and watch for changes. |
| `astrosearch-signals` | `signals.py` | Process telescope-delivery JSONL against reference signals and commit output snapshots. |
| `astrosearch-plot-catalogs` | `astronomy.py` | Plot catalog sources from a query or saved dataset. |
| `astrosearch-tess` | `tess.py` | Retrieve TESS SPOC products or plot local FITS light curves. |

## Consolidation map

| Previous modules | Consolidated module |
|---|---|
| `models.py`, `providers.py`, `crossmatch.py` | `core.py` |
| `astronomy_pipeline.py`, `plot_catalog_sources.py` | `astronomy.py` |
| `representations.py`, `signal_pipeline.py`, `signal_plotting.py` | `signals.py` |
| `tess_adapter.py`, `tess_service.py`, `tess_cli.py` | `tess.py` |
| `datasets.py`, `api.py`, `main.py`, `astronomy.py` | Retained under the same names. |

Direct imports should now use `core`, `signals`, or `tess` for the consolidated functionality. The installed command names did not change. `python -m astronomy` and `python -m signals` replace the former module invocations for those workflows.

## Boundaries

- UI-ready API workflows cover catalog discovery, search, object/system summaries, datasets, saved query execution, single-observation signal cross-reference, and TESS retrieval/plotting. See [frontend integration](FRONTEND_INTEGRATION.md) for request behavior and data formats.
- Local-file plotting, bulk signal delivery snapshot commits, and Mantis publish/watch operations remain operator CLI workflows; their Python capabilities and commands remain available.
- General archive-media retrieval is not present: TESS SPOC light curves are the integrated media fetch path; image cutouts and general spectra are not.
- Signal novelty output is a review aid and does not establish a discovery.
- The Mantis observatory extension is a manifest/README scaffold with no functioning dashboard panel.
- The API runs through `astrosearch serve` or ASGI import; it has no separate installed server command.
