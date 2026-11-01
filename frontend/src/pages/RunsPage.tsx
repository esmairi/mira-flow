import { useQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router-dom";
import { StatusBadge } from "../components/StatusBadge";
import { Button, Card, Empty, ErrorBox, Loading, RefreshButton, Td, Th } from "../components/ui";
import { api } from "../lib/api";
import { fmtDuration, fmtTime, shortId } from "../lib/format";
import { RUN_STATUSES } from "../lib/status";

const PAGE = 50;

export function RunsPage() {
  const [params, setParams] = useSearchParams();
  const workflow_name = params.get("workflow") ?? "";
  const status = params.get("status") ?? "";
  const offset = Number(params.get("offset") ?? 0);

  const set = (k: string, v: string) => {
    const next = new URLSearchParams(params);
    if (v) next.set(k, v);
    else next.delete(k);
    if (k !== "offset") next.delete("offset");
    setParams(next);
  };

  const wfs = useQuery({ queryKey: ["workflows"], queryFn: api.workflows });
  const runs = useQuery({
    queryKey: ["runs", { workflow_name, status, offset, limit: PAGE }],
    queryFn: () => api.runs({ workflow_name, status, offset, limit: PAGE }),
  });

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Runs</h1>
        <RefreshButton />
      </div>
      <div className="flex flex-wrap gap-2">
        <select value={workflow_name} onChange={(e) => set("workflow", e.target.value)} className="rounded-md border border-slate-300 bg-white px-2 py-1.5 text-sm">
          <option value="">All workflows</option>
          {wfs.data?.map((w) => (
            <option key={w.name}>{w.name}</option>
          ))}
        </select>
        <select value={status} onChange={(e) => set("status", e.target.value)} className="rounded-md border border-slate-300 bg-white px-2 py-1.5 text-sm">
          <option value="">All statuses</option>
          {RUN_STATUSES.map((s) => (
            <option key={s}>{s}</option>
          ))}
        </select>
      </div>
      <ErrorBox error={runs.error} />
      <Card>
        {runs.isPending ? (
          <Loading />
        ) : !runs.data?.length ? (
          <Empty>No runs match.</Empty>
        ) : (
          <RunsTable runs={runs.data} showWorkflow />
        )}
        <div className="mt-3 flex items-center justify-end gap-2 text-sm text-slate-500">
          <span>
            {offset + 1}–{offset + (runs.data?.length ?? 0)}
          </span>
          <Button disabled={offset === 0} onClick={() => set("offset", String(Math.max(0, offset - PAGE)))}>
            ‹ Prev
          </Button>
          <Button disabled={(runs.data?.length ?? 0) < PAGE} onClick={() => set("offset", String(offset + PAGE))}>
            Next ›
          </Button>
        </div>
      </Card>
    </div>
  );
}

export function RunsTable({ runs, showWorkflow }: { runs: import("../lib/types").WorkflowRunSummary[]; showWorkflow?: boolean }) {
  return (
    <table className="w-full">
      <thead className="border-b border-slate-200">
        <tr>
          <Th>Run</Th>
          {showWorkflow && <Th>Workflow</Th>}
          <Th>Status</Th>
          <Th>Created</Th>
          <Th>Started</Th>
          <Th>Duration</Th>
        </tr>
      </thead>
      <tbody className="divide-y divide-slate-100">
        {runs.map((r) => (
          <tr key={r.id} className="hover:bg-slate-50">
            <Td>
              <Link to={`/runs/${r.id}`} className="font-mono text-sky-700 hover:underline">
                {shortId(r.id)}
              </Link>
            </Td>
            {showWorkflow && (
              <Td>
                <Link to={`/workflows/${encodeURIComponent(r.workflow_name)}`} className="hover:underline">
                  {r.workflow_name}
                </Link>{" "}
                <span className="text-xs text-slate-400">v{r.workflow_version}</span>
              </Td>
            )}
            <Td>
              <StatusBadge status={r.status} />
            </Td>
            <Td className="text-slate-600">{fmtTime(r.created_at)}</Td>
            <Td className="text-slate-600">{fmtTime(r.started_at)}</Td>
            <Td className="text-slate-600">{fmtDuration(r.started_at, r.ended_at)}</Td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
