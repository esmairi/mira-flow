import type { ReactNode } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router-dom";
import { JsonView } from "../components/JsonView";
import { StatusBadge } from "../components/StatusBadge";
import { WorkflowGraph } from "../components/WorkflowGraph";
import { Button, Card, Empty, ErrorBox, Loading, Modal, RefreshButton, Td, Th } from "../components/ui";
import { api } from "../lib/api";
import { fmtDuration, fmtTime, shortId } from "../lib/format";
import { isTerminal } from "../lib/status";
import { executionPath, latestAttempts } from "../lib/tasks";

export function RunPage() {
  const { runId = "" } = useParams();
  const [params, setParams] = useSearchParams();
  const selectedTask = params.get("task");
  const qc = useQueryClient();

  const run = useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.run(runId),
  });
  const tasks = useQuery({ queryKey: ["tasks", runId], queryFn: () => api.tasks(runId) });
  const wf = useQuery({
    queryKey: ["workflow", run.data?.workflow_name],
    queryFn: () => api.workflow(run.data!.workflow_name),
    enabled: !!run.data,
  });

  const cancel = useMutation({
    mutationFn: () => api.cancel(runId),
    onSuccess: (data) => {
      qc.setQueryData(["run", runId], data);
      qc.invalidateQueries({ queryKey: ["runs"] });
      qc.invalidateQueries({ queryKey: ["grid"] });
    },
  });

  if (run.isPending) return <Loading />;
  if (run.error) return <ErrorBox error={run.error} />;
  const r = run.data;
  const latest = tasks.data ? latestAttempts(tasks.data) : {};
  const statusByTask = Object.fromEntries(Object.entries(latest).map(([k, v]) => [k, v.status]));
  const openTask = (id: string | null) => setParams(id ? { task: id } : {});

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <div className="text-xs text-slate-500">
            <Link to={`/workflows/${encodeURIComponent(r.workflow_name)}`} className="hover:underline">
              {r.workflow_name}
            </Link>{" "}
            / runs /
          </div>
          <h1 className="flex items-center gap-3 text-xl font-semibold">
            <span className="font-mono">{shortId(r.id)}</span> <StatusBadge status={r.status} />
          </h1>
        </div>
        <div className="flex gap-2">
        <RefreshButton />
        <Button
          variant="danger"
          disabled={isTerminal(r.status) || cancel.isPending}
          onClick={() => confirm(`Cancel run ${shortId(r.id)}? In-flight attempts are not interrupted.`) && cancel.mutate()}
        >
          ■ Cancel run
        </Button>
        </div>
      </div>
      <ErrorBox error={cancel.error} />

      <Card>
        <dl className="grid grid-cols-2 gap-x-6 gap-y-3 text-sm md:grid-cols-4">
          <Field label="Run ID" mono>{r.id}</Field>
          <Field label="Workflow">{r.workflow_name} v{r.workflow_version}</Field>
          <Field label="Correlation ID" mono>{r.correlation_id ?? "—"}</Field>
          <Field label="Idempotency key" mono>{r.idempotency_key ?? "—"}</Field>
          <Field label="Created">{fmtTime(r.created_at)}</Field>
          <Field label="Started">{fmtTime(r.started_at)}</Field>
          <Field label="Ended">{fmtTime(r.ended_at)}</Field>
          <Field label="Duration">{fmtDuration(r.started_at, r.ended_at)}</Field>
        </dl>
      </Card>

      {wf.data && (
        <Card title="Graph">
          <WorkflowGraph
            jobs={wf.data.jobs}
            statusByTask={statusByTask}
            path={tasks.data ? executionPath(tasks.data) : undefined}
            onSelect={(name) => latest[name] && openTask(latest[name].id)}
          />
        </Card>
      )}

      <Card title="Task attempts">
        {tasks.isPending ? (
          <Loading />
        ) : !tasks.data?.length ? (
          <Empty>No task attempts yet.</Empty>
        ) : (
          <table className="w-full">
            <thead className="border-b border-slate-200">
              <tr>
                <Th>Task</Th>
                <Th>Attempt</Th>
                <Th>Status</Th>
                <Th>Started</Th>
                <Th>Duration</Th>
                <Th>Next retry</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {tasks.data.map((t) => (
                <tr key={t.id} onClick={() => openTask(t.id)} className="cursor-pointer hover:bg-slate-50">
                  <Td className="font-medium text-sky-700">{t.task_name}</Td>
                  <Td>#{t.attempt}</Td>
                  <Td>
                    <StatusBadge status={t.status} />
                  </Td>
                  <Td className="text-slate-600">{fmtTime(t.started_at)}</Td>
                  <Td className="text-slate-600">{fmtDuration(t.started_at, t.ended_at)}</Td>
                  <Td className="text-slate-600">{fmtTime(t.next_retry_at)}</Td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>

      <Card title="Payloads">
        <div className="grid gap-4 lg:grid-cols-3">
          <JsonView label="Input" value={r.input} />
          <JsonView label="Output" value={r.output} />
          <JsonView label="Error" value={r.error} />
        </div>
      </Card>

      {selectedTask && <TaskModal runId={runId} taskId={selectedTask} runCancelled={r.status === "CANCELLED"} onClose={() => openTask(null)} />}
    </div>
  );
}

function Field({ label, mono, children }: { label: string; mono?: boolean; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-slate-500">{label}</dt>
      <dd className={`truncate ${mono ? "font-mono text-xs" : ""}`} title={typeof children === "string" ? children : undefined}>
        {children}
      </dd>
    </div>
  );
}

function TaskModal({ runId, taskId, runCancelled, onClose }: { runId: string; taskId: string; runCancelled: boolean; onClose: () => void }) {
  const qc = useQueryClient();
  const task = useQuery({ queryKey: ["task", runId, taskId], queryFn: () => api.task(runId, taskId) });
  const retry = useMutation({
    mutationFn: () => api.retry(runId, taskId),
    onSuccess: () => {
      qc.invalidateQueries({ queryKey: ["tasks", runId] });
      qc.invalidateQueries({ queryKey: ["run", runId] });
      qc.invalidateQueries({ queryKey: ["grid"] });
    },
  });
  const t = task.data;

  return (
    <Modal title={t ? `${t.task_name} · attempt #${t.attempt}` : "Task"} onClose={onClose}>
      {task.isPending ? (
        <Loading />
      ) : task.error ? (
        <ErrorBox error={task.error} />
      ) : (
        t && (
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <StatusBadge status={t.status} />
              <Button
                variant="primary"
                disabled={t.status !== "FAILED" || runCancelled || retry.isPending}
                title={t.status !== "FAILED" ? "Only FAILED attempts can be retried" : runCancelled ? "Run is cancelled" : ""}
                onClick={() => retry.mutate()}
              >
                ↻ Retry
              </Button>
            </div>
            <ErrorBox error={retry.error} />
            {retry.data && (
              <div className="rounded-md border border-green-200 bg-green-50 px-3 py-2 text-sm text-green-800">
                Retry published to <span className="font-mono">{retry.data.topic}</span> — a running orchestrator will pick it up.
              </div>
            )}
            <dl className="grid grid-cols-2 gap-3 text-sm">
              <Field label="Task run ID" mono>{t.id}</Field>
              <Field label="Scheduled">{fmtTime(t.scheduled_at)}</Field>
              <Field label="Started">{fmtTime(t.started_at)}</Field>
              <Field label="Ended">{fmtTime(t.ended_at)}</Field>
              <Field label="Duration">{fmtDuration(t.started_at, t.ended_at)}</Field>
              <Field label="Next retry">{fmtTime(t.next_retry_at)}</Field>
            </dl>
            <JsonView label="Input" value={t.input} />
            <JsonView label="Output" value={t.output} />
            <JsonView label="Error" value={t.error} />
          </div>
        )
      )}
    </Modal>
  );
}
