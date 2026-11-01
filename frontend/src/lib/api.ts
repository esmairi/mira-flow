import type {
  GridResponse,
  RetryResponse,
  TaskRunDetail,
  TaskRunSummary,
  TriggerResponse,
  WorkflowRunDetail,
  WorkflowRunSummary,
  WorkflowSummary,
  Json,
} from "./types";

const BASE = (import.meta.env.VITE_API_BASE as string | undefined)?.replace(/\/$/, "") ?? "/api";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(BASE + path, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = await res.json();
      detail = typeof body.detail === "string" ? body.detail : JSON.stringify(body.detail ?? body);
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, `${res.status}: ${detail}`);
  }
  return res.json() as Promise<T>;
}

const qs = (params: Record<string, string | number | undefined>) => {
  const s = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) if (v !== undefined && v !== "") s.set(k, String(v));
  const str = s.toString();
  return str ? `?${str}` : "";
};

export const api = {
  ready: () => request<{ status: string }>("/ready"),
  workflows: () => request<WorkflowSummary[]>("/workflows"),
  workflow: (name: string) => request<WorkflowSummary>(`/workflows/${encodeURIComponent(name)}`),
  grid: (name: string, limit: number, offset: number) =>
    request<GridResponse>(`/workflows/${encodeURIComponent(name)}/grid${qs({ limit, offset })}`),
  trigger: (name: string, idempotency_key: string, input: Json) =>
    request<TriggerResponse>(`/workflows/${encodeURIComponent(name)}/trigger`, {
      method: "POST",
      body: JSON.stringify({ idempotency_key, input }),
    }),
  runs: (p: { workflow_name?: string; status?: string; limit?: number; offset?: number }) =>
    request<WorkflowRunSummary[]>(`/runs${qs(p)}`),
  run: (id: string) => request<WorkflowRunDetail>(`/runs/${id}`),
  cancel: (id: string) => request<WorkflowRunDetail>(`/runs/${id}/cancel`, { method: "POST" }),
  tasks: (runId: string) => request<TaskRunSummary[]>(`/runs/${runId}/tasks`),
  task: (runId: string, taskId: string) => request<TaskRunDetail>(`/runs/${runId}/tasks/${taskId}`),
  retry: (runId: string, taskId: string) =>
    request<RetryResponse>(`/runs/${runId}/tasks/${taskId}/retry`, { method: "POST" }),
};
