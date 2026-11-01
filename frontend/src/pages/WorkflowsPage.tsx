import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link } from "react-router-dom";
import { StatusDot } from "../components/StatusBadge";
import { TriggerDialog } from "../components/TriggerDialog";
import { Button, Card, Empty, ErrorBox, Loading, RefreshButton, Td, Th } from "../components/ui";
import { api } from "../lib/api";
import { fmtTime } from "../lib/format";
import type { WorkflowSummary } from "../lib/types";

function RecentRuns({ workflow }: { workflow: string }) {
  const runs = useQuery({
    queryKey: ["runs", { workflow_name: workflow, limit: 10 }],
    queryFn: () => api.runs({ workflow_name: workflow, limit: 10 }),
  });
  if (!runs.data) return <span className="text-slate-300">…</span>;
  if (!runs.data.length) return <span className="text-xs text-slate-400">no runs</span>;
  return (
    <div className="flex items-center gap-1">
      {[...runs.data].reverse().map((r) => (
        <Link key={r.id} to={`/runs/${r.id}`}>
          <StatusDot status={r.status} title={`${r.status} · ${fmtTime(r.created_at)}`} />
        </Link>
      ))}
    </div>
  );
}

export function WorkflowsPage() {
  const [filter, setFilter] = useState("");
  const [triggering, setTriggering] = useState<WorkflowSummary | null>(null);
  const wfs = useQuery({ queryKey: ["workflows"], queryFn: api.workflows });

  const rows = (wfs.data ?? []).filter((w) => w.name.toLowerCase().includes(filter.toLowerCase()));

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold">Workflows</h1>
        <div className="flex items-center gap-2">
          <input
          placeholder="Search workflows…"
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          className="w-64 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-sm focus:border-sky-500 focus:outline-none"
          />
          <RefreshButton />
        </div>
      </div>
      <ErrorBox error={wfs.error} />
      <Card>
        {wfs.isPending ? (
          <Loading />
        ) : !rows.length ? (
          <Empty>No workflows configured.</Empty>
        ) : (
          <table className="w-full">
            <thead className="border-b border-slate-200">
              <tr>
                <Th>Workflow</Th>
                <Th>Version</Th>
                <Th>Jobs</Th>
                <Th>Entry</Th>
                <Th>Recent runs</Th>
                <Th className="text-right">Actions</Th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {rows.map((w) => (
                <tr key={w.name} className="hover:bg-slate-50">
                  <Td>
                    <Link to={`/workflows/${encodeURIComponent(w.name)}`} className="font-medium text-sky-700 hover:underline">
                      {w.name}
                    </Link>
                  </Td>
                  <Td>v{w.version}</Td>
                  <Td>{w.jobs.length}</Td>
                  <Td className="font-mono text-xs text-slate-600">
                    {w.entry_consumer ?? <span className="text-slate-400">external</span>}
                  </Td>
                  <Td>
                    <RecentRuns workflow={w.name} />
                  </Td>
                  <Td className="text-right">
                    <Button
                      onClick={() => setTriggering(w)}
                      disabled={!w.entry_consumer}
                      title={w.entry_consumer ? "Trigger a new run" : "No entry_consumer — started by an external service"}
                    >
                      ▶ Trigger
                    </Button>
                  </Td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </Card>
      {triggering && <TriggerDialog workflow={triggering.name} onClose={() => setTriggering(null)} />}
    </div>
  );
}
