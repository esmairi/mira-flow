import type { TaskRunSummary } from "./types";

/** Latest attempt per task_name. */
export function latestAttempts(tasks: TaskRunSummary[]): Record<string, TaskRunSummary> {
  const out: Record<string, TaskRunSummary> = {};
  for (const t of tasks) if (!out[t.task_name] || t.attempt > out[t.task_name].attempt) out[t.task_name] = t;
  return out;
}

/** Task names in the order the run first reached them. A run is always a
 *  single chain (first-match routing, one consumer per topic), so
 *  consecutive names are exactly the edges the run travelled. */
export function executionPath(tasks: TaskRunSummary[]): string[] {
  const firstStart = new Map<string, string>();
  for (const t of tasks) {
    const at = t.started_at ?? "";
    const prev = firstStart.get(t.task_name);
    if (prev === undefined || at < prev) firstStart.set(t.task_name, at);
  }
  return [...firstStart.entries()].sort((a, b) => (a[1] < b[1] ? -1 : a[1] > b[1] ? 1 : 0)).map(([n]) => n);
}
