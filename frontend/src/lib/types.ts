export type RunStatus = "PENDING" | "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED";
export type TaskStatus = "RUNNING" | "SUCCEEDED" | "UP_FOR_RETRY" | "FAILED";
export type Json = Record<string, unknown>;

export type Condition =
  | { field: string; op: "eq" | "ne" | "gt" | "gte" | "lt" | "lte" | "in" | "contains"; value: unknown }
  | { all: Condition[] }
  | { any: Condition[] };

export interface RouteSummary {
  name: string;
  topic: string;
  when: Condition | null;
}

export interface JobSummary {
  name: string;
  handler: string;
  consumer: string;
  input_topic: string;
  producer_routes: string[];
  output_topics: string[];
  routes?: RouteSummary[];
  retries: number;
  retry_delay_seconds: number;
  is_entry: boolean;
}

export interface WorkflowSummary {
  name: string;
  version: number;
  entry_consumer: string | null;
  jobs: JobSummary[];
}

export interface WorkflowRunSummary {
  id: string;
  workflow_name: string;
  workflow_version: number;
  status: RunStatus;
  correlation_id: string | null;
  created_at: string;
  started_at: string | null;
  ended_at: string | null;
}

export interface WorkflowRunDetail extends WorkflowRunSummary {
  idempotency_key: string | null;
  input: Json;
  output: Json | null;
  error: Json | null;
  lock_version: number;
}

export interface TaskRunSummary {
  id: string;
  workflow_run_id: string;
  task_name: string;
  attempt: number;
  status: TaskStatus;
  correlation_id: string | null;
  scheduled_at: string | null;
  started_at: string | null;
  ended_at: string | null;
  next_retry_at: string | null;
}

export interface TaskRunDetail extends TaskRunSummary {
  input: Json | null;
  output: Json | null;
  error: Json | null;
}

export interface GridRun extends WorkflowRunSummary {
  /** Latest attempt of every task that has started in this run. */
  tasks: TaskRunSummary[];
}

export interface GridResponse {
  runs: GridRun[];
  has_more: boolean;
}

export interface TriggerResponse {
  workflow_run_id: string;
  correlation_id: string | null;
  topic: string;
  /** null when nothing was published (repeated key for a started run). */
  message_id: string | null;
  created: boolean;
  published: boolean;
}

export interface RetryResponse {
  task_run_id: string;
  workflow_run_id: string;
  topic: string;
  message_id: string;
}
