# frontend — Airflow-style UI for the api_server

React + Vite + Tailwind (v4) + TanStack Query. Talks to
[`packages/api_server`](../packages/api_server/src/mira_flow/api_server/README.md).

## Run it

```bash
# 1. the API (from the repo root)
uv run uvicorn mira_flow.api_server.app:app --reload --port 8001

# 2. the UI
cd frontend
npm install
npm run dev        # http://localhost:5173
```

In dev, the UI calls `/api/*` and Vite proxies it to `API_SERVER_URL`
(default `http://localhost:8001`, see `.env.example`). For a production build
(`npm run build` → `dist/`) served from another origin than the API, set
`VITE_API_BASE` at build time (the API already allows any CORS origin).

## Pages

| Path | |
|---|---|
| `/` | Workflows — version, job count, entry consumer, last-10-runs status dots, **Trigger** |
| `/workflows/:name` | Tabs: **Grid** (last 25 runs × jobs, latest attempt per cell), **Graph** (jobs linked by topic, colored by the latest run), **Runs**, **Details** (handlers, topics, routes, retry policy) |
| `/runs` | All runs, filter by workflow/status, paginated |
| `/runs/:id` | Run metadata, **Cancel**, graph, every task attempt, input/output/error. Click an attempt for its payloads and **Retry** (FAILED only) |

Run/task views poll every 3–5s; a finished run stops polling.
