# Frontend integration

## API surface for a UI

The browser should send JSON to the versioned FastAPI routes; Python modules and command-line programs remain backend/operator interfaces. The principal UI flows are:

| UI task | API route | Result |
|---|---|---|
| Discover catalogs and available parameters | `GET /api/v1/catalogs`, `GET /api/v1/catalogs/{name}` | JSON |
| Search by coordinates or object name | `POST /api/v1/search` | Crossmatch evidence and provenance as JSON |
| Summarize an object or extrasolar system | `POST /api/v1/summaries/object`, `POST /api/v1/summaries/system` | Evidence-based summary JSON |
| Save, inspect, and rerun a search | `POST /api/v1/queries`, `GET /api/v1/queries/{id}`, `POST /api/v1/queries/{id}/run` | Saved query or crossmatch JSON |
| Build and retrieve a dataset | `POST /api/v1/datasets/create`, then poll `GET /api/v1/datasets/{id}` and download `/export` | `202` job metadata, then status and file |
| Cross-reference a supplied signal | `POST /api/v1/signals/cross-reference` | Representation, comparison evidence, and review status as JSON |
| Retrieve or plot TESS SPOC light curves | Routes described below | Canonical observation JSON or PNG |

Search accepts exactly one target form. For example:

```json
{"name":"M87","radius_arcsec":5,"profile":"optical"}
```

or:

```json
{"ra":187.70593,"dec":12.39112,"radius_arcsec":5,"catalogs":["gaia_dr3"]}
```

Search input validation errors use HTTP `422` with a `detail` field. Missing resources use `404`; upstream archive failures use `502`; rate limits use `429`. Dataset creation is asynchronous: use the `Location` header in the `202` response to poll for completion, then download the export. The interactive OpenAPI document at `/api/docs` is the live request-schema reference.

For cross-origin browser access, set `CORS_ORIGINS` to the frontend origin. Browser preflight requests are allowed through to CORS handling; actual requests remain subject to configured authentication and rate limits. Do not ship a privileged API key in a public browser bundle; use an authenticated backend/proxy for production deployments.

Some operator workflows remain CLI-only because they act on local files or external Mantis spaces: local-file plotting, committing bulk signal delivery snapshots, and Mantis publish/watch operations. Their computation modules remain available; the API routes above cover interactive search, summaries, datasets, signal cross-reference, and TESS retrieval/plotting.

---

## TESS light curves


## What exists in the backend

```text
Frontend
   | JSON POST: retrieve data or render a plot
   v
FastAPI routes in api.py
   |
   +--> tess.py --> MAST query, product cache/download
   |                         |
   |                         v
   |                   FITS adapter in tess.py --> canonical observations
   |
   +--> signals.py --> PNG response
```

- The TESS section in `tess.py` retrieves public SPOC products from MAST, applies sector/product filters, uses the persistent cache, checks target identity, and returns canonical observations.
- The FITS adapter in `tess.py` preserves flux, uncertainties, quality flags, and provenance.
- The plotting functions in `signals.py` create PNGs from canonical observations. They highlight nonzero quality flags by default and apply normalization or phase folding only to the displayed figure.
- The CLI in `tess.py` handles retrieval, local FITS plotting, and JSONL export. A frontend should call the API instead of launching this CLI.
- The catalog plotting functions in `astronomy.py` make position and measurement plots. They do not plot time series.

## Start the backend locally

From the repository root, install the project and run the API:

```sh
pip install -e '.[dev]'
python main.py serve --host 127.0.0.1 --port 8000
```

API documentation is available at `http://127.0.0.1:8000/api/docs`. Set the frontend API base URL to `http://127.0.0.1:8000`, or to the deployed AstroSearch service URL.

For a cross-origin development frontend, set `CORS_ORIGINS` on the backend, for example:

```text
CORS_ORIGINS=http://localhost:5173
```

When CORS is enabled, browser preflight `OPTIONS` requests are handled before API-key checks. The actual data request still uses the configured authentication and rate limits.

The API middleware accepts an API key in `X-API-Key` or a bearer token in `Authorization`. Do not put a long-lived privileged key in a public browser bundle. For authenticated deployments, use the frontend's authenticated backend/proxy path. Local development can use the API's configured local auth behavior.

## Choose the frontend integration path

Use the **PNG route** when the UI needs a rendered preview with minimal charting code. It returns the server-rendered figure as an `image/png` response.

Use the **JSON route** when the UI needs to render an interactive chart, inspect sectors and provenance, support client-side controls, or export data. It returns one canonical observation per selected product. The frontend chart should treat `null` samples as gaps and keep `quality` flags available for highlighting or filtering in the display.

The routes are separate requests. If a screen needs both canonical arrays and the server PNG, it can call both; the backend shares the FITS cache, but each API request performs its own MAST observation/product query. Prefer one route when it meets the screen's needs.

## Request contract

Both routes take a JSON body with exactly one target form:

```json
{"target": {"tic_id": "141914082"}}
```

```json
{"target": {"name": "Vega"}}
```

```json
{"target": {"ra_deg": 279.2347, "dec_deg": 38.7837}}
```

Names are resolved by the backend through its configured CDS Sesame resolver. The frontend does not need to resolve names itself. TIC identifiers may be sent as digits or with a `TIC` prefix.

Shared selection fields:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `sectors` | integer array | omitted | Select sectors; omit to retrieve all available sectors. |
| `flux_kind` | `SAP_FLUX` or `PDCSAP_FLUX` | `PDCSAP_FLUX` | Flux column to read. There is no silent fallback if it is unavailable. |
| `radius_arcsec` | positive number, at most 3600 | `5` | Coordinate cone and FITS target-position tolerance. |

Example retrieval body:

```json
{
  "target": {"tic_id": "141914082"},
  "sectors": [1, 2],
  "flux_kind": "PDCSAP_FLUX"
}
```

The request schema rejects extra fields, multiple target forms, one-sided coordinate pairs, duplicate/nonpositive sectors, and invalid flux kinds.

## Route 1: retrieve canonical observations

```http
POST /api/v1/signals/tess/light-curves
Content-Type: application/json
```

Successful response example (arrays abbreviated):

```json
{
  "status": "partial",
  "target": {"tic_id": "141914082"},
  "retrieved_at": "2026-09-27T12:00:00+00:00",
  "selection": {
    "sectors": [1, 2],
    "flux_kind": "PDCSAP_FLUX",
    "radius_arcsec": 5.0,
    "pipeline": "SPOC"
  },
  "product_count": 2,
  "observation_count": 1,
  "cache": {"cache_hits": 1, "downloads": 1},
  "observations": [
    {
      "observation_id": "tess-product-…",
      "object_id": "TIC 141914082",
      "title": "TIC 141914082, TESS sector 1 (PDCSAP_FLUX)",
      "modality": "light_curve",
      "ra_deg": 94.6175354,
      "dec_deg": -72.0448462,
      "axis": [58324.8, 58324.82, null],
      "values": [1.02, 1.01, null],
      "uncertainties": [0.01, 0.01, null],
      "quality": [0, 0, 8],
      "instrument": "TESS",
      "band": "TESS",
      "source_url": "https://mast.stsci.edu/api/v0.1/Download/file?uri=…",
      "provenance": {
        "archive": "MAST",
        "pipeline": "SPOC",
        "sector": 1,
        "value_kind": "PDCSAP_FLUX",
        "value_unit": "electron/s",
        "time_format": "BTJD",
        "time_scale": "TDB",
        "data_uri": "mast:TESS/product/…_lc.fits",
        "product_filename": "…_lc.fits",
        "cache_hit": true
      }
    }
  ],
  "failures": [
    {
      "sector": 2,
      "product_id": "mast:TESS/product/…_lc.fits",
      "error_type": "product_unavailable",
      "message": "…"
    }
  ]
}
```

The real arrays contain one value per FITS sample; the abbreviated example shows `null` for non-finite values. Keep the arrays paired by index. `quality` contains the raw FITS quality flags; `0` is unflagged. The API does not discard flagged samples. BTJD/TDB metadata is supplied by the adapter where available.

Retrieval `status` values:

| Status | Frontend behavior |
|---|---|
| `complete` | Show all returned observations. |
| `partial` | Show successful observations and display the per-product failures. |
| `no_products` | Show an empty-state message. This JSON route returns HTTP 200 with empty observations. |
| `failed` | Normally delivered as HTTP 502 with the result under `detail`; show a retryable error. |

## Route 2: retrieve and render a PNG

```http
POST /api/v1/signals/tess/light-curves/plot
Content-Type: application/json
```

This route accepts the shared selection fields plus:

| Field | Type | Default | Meaning |
|---|---|---|---|
| `normalize` | boolean | `false` | Divide displayed flux and uncertainty by the median. Raw JSON data is not changed. |
| `show_uncertainties` | boolean | `false` | Draw error bars where uncertainties are finite. |
| `quality_display` | `highlight` or `hide` | `highlight` | Show flagged samples as distinct points, or omit them from the image only. |
| `period_days` | positive number or null | null | Fold the plot on this caller-supplied period. No period is inferred. |
| `epoch_btjd` | number or null | null | Optional phase-zero epoch; requires `period_days`. |

Example:

```json
{
  "target": {"tic_id": "141914082"},
  "sectors": [1, 2],
  "flux_kind": "PDCSAP_FLUX",
  "normalize": false,
  "show_uncertainties": true,
  "quality_display": "highlight",
  "period_days": null,
  "epoch_btjd": null
}
```

On success, the response body is PNG bytes (`Content-Type: image/png`), not JSON or a URL. The `X-AstroSearch-Retrieval-Status` response header is `complete` or `partial`. Use `response.blob()` and an object URL to display it. A plot with no observations returns HTTP 404; all-product failures return HTTP 502.

## Browser client examples

The following TypeScript helpers work with React, Vue, Svelte, or plain browser code. Keep the API URL configurable for development and deployment.

```ts
const API_BASE_URL = import.meta.env.VITE_ASTROSEARCH_API_URL ?? "http://127.0.0.1:8000";

type TessTarget =
  | { tic_id: string }
  | { name: string }
  | { ra_deg: number; dec_deg: number };

type TessSelection = {
  target: TessTarget;
  sectors?: number[];
  flux_kind?: "SAP_FLUX" | "PDCSAP_FLUX";
  radius_arcsec?: number;
};

type SignalObservation = {
  observation_id: string;
  object_id: string;
  title?: string;
  modality: "light_curve";
  ra_deg: number;
  dec_deg: number;
  axis: Array<number | null>;
  values: Array<number | null>;
  uncertainties?: Array<number | null>;
  quality?: number[];
  instrument: string;
  band: string;
  source_url?: string;
  provenance: Record<string, unknown>;
};

type TessRetrieval = {
  status: "complete" | "partial" | "no_products" | "failed";
  target: Record<string, unknown>;
  observations: SignalObservation[];
  failures: Array<{
    sector: number | null;
    product_id: string | null;
    error_type: string;
    message: string;
  }>;
};

async function postJson<T>(path: string, body: unknown, signal?: AbortSignal): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!response.ok) {
    const errorBody = await response.json().catch(() => null);
    const detail = errorBody?.detail?.message ?? errorBody?.detail ?? `HTTP ${response.status}`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  return response.json() as Promise<T>;
}

export function retrieveTess(selection: TessSelection, signal?: AbortSignal) {
  return postJson<TessRetrieval>("/api/v1/signals/tess/light-curves", selection, signal);
}

export async function fetchTessPlot(
  selection: TessSelection & {
    normalize?: boolean;
    show_uncertainties?: boolean;
    quality_display?: "highlight" | "hide";
    period_days?: number | null;
    epoch_btjd?: number | null;
  },
  signal?: AbortSignal,
): Promise<{ url: string; status: string }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/signals/tess/light-curves/plot`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(selection),
    signal,
  });
  if (!response.ok) {
    const errorBody = await response.json().catch(() => null);
    const detail = errorBody?.detail?.message ?? errorBody?.detail ?? `HTTP ${response.status}`;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }
  const blob = await response.blob();
  if (!blob.type.includes("image/png")) throw new Error(`Expected PNG, received ${blob.type || "unknown content type"}`);
  return {
    url: URL.createObjectURL(blob),
    status: response.headers.get("X-AstroSearch-Retrieval-Status") ?? "complete",
  };
}
```

When a component replaces or unmounts a plot, call `URL.revokeObjectURL(url)` for the previous object URL. For an image, render `<img src={url} alt="TESS light curve" />`. Do not set the plot route as an `<img src>` directly: it is a POST endpoint with a JSON body.

For a chart library, map `axis[i]` to `values[i]`, preserve `null` values as gaps, and map `quality[i] !== 0` to the flagged-point style. When the user chooses to hide flagged samples, filter only the chart's display arrays; leave the retrieved observation intact.

## UI states and interaction notes

1. Collect one target form: TIC, name, or RA/Dec. Disable conflicting target inputs so the request cannot combine forms.
2. Let the user select one or more sectors. If the user explicitly chooses “all available,” omit `sectors`; consider showing a loading state because it may return many arrays.
3. Choose flux kind. Preserve this choice when changing plot controls.
4. Show loading/cancel state while the POST is in flight. Use `AbortController` when a target or sector selection changes.
5. For JSON, display each observation separately or group by `provenance.sector`; list `failures` alongside successful curves for partial responses.
6. For no products, show “No TESS SPOC light curves found for this selection.” Do not show a blank graph as if it were a measurement.
7. For an error response, show its `detail` and offer retry. Treat HTTP 422 as request correction, 404 name resolution/no plot as not found, and 502/503 as a retryable service/archive issue.
8. Make normalization, quality display, and phase options visibly display-only. Require a user-entered period before enabling phase folding; only then enable an optional BTJD epoch field.

## Observatory extension status

The current `mantis-extension/astrosearch-observatory` directory contains its extension manifest and overview README, but no panel source or built `dist/panel.js` in this checkout. Its manifest grants `maps:read` and `selection:read`, and its README currently describes no backend/network access. Connecting that panel requires implementing/building the panel UI and following the host's network-permission and authentication model. The API examples above describe the AstroSearch side of that integration; they do not add network access to the extension.

## Backend files and ownership

| File | Frontend relevance |
|---|---|
| `api.py` | HTTP request schemas, routes, response/error behavior, auth middleware. |
| `tess.py` | MAST search/download/cache, FITS-to-observation conversion, and operator CLI; frontend should use the API. |
| `signals.py` | Signal representation, cross-reference, and server PNG renderer used by the plot endpoint. |
| `astronomy.py` | Catalog position/measurement plots, separate from TESS time-series plots. |
| `LIGHT_CURVE_RETRIEVAL_PLAN.md` | Backend workflow, cache behavior, CLI, and data-flow details. |
