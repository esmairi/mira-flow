import { useQuery } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { StatusDot } from "../components/StatusBadge";
import { TriggerDialog } from "../components/TriggerDialog";
import { WorkflowGraph } from "../components/WorkflowGraph";
import { Button, Card, Empty, ErrorBox, Loading, RefreshButton, Td, Th } from "../components/ui";
import { api } from "../lib/api";
import { fmtTime, shortId } from "../lib/format";
import { styleFor } from "../lib/status";
import { executionPath, latestAttempts } from "../lib/tasks";
import type { GridRun, WorkflowRunSummary, WorkflowSummary } from "../lib/types";
import { RunsTable } from "./RunsPage";

const TABS = ["grid", "graph", "runs", "details"] as const;
type Tab = (typeof TABS)[number];
/** Runs per page in the grid and runs tabs. */
const PAGE_SIZE = 10;

export function WorkflowPage() {
  const { name = "" } = useParams();
  const [params, setParams] = useSearchParams();
  const tab = (TABS.includes(params.get("tab") as Tab) ? params.get("tab") : "grid") as Tab;
  const [triggering, setTriggering] = useState(false);

  const wf = useQuery({ queryKey: ["workflow", name], queryFn: () => api.workflow(name) });
  const offset = Math.max(0, Number(params.get("offset") ?? 0) || 0);
  // Grid: runs + each task's latest attempt, one request per page.
  const grid = useQuery({
    queryKey: ["grid", name, { limit: PAGE_SIZE, offset }],
    queryFn: () => api.grid(name, PAGE_SIZE, offset),
    enabled: tab === "grid",
  });
  // Runs tab: plain run list; one extra row tells whether an older page exists.
  const runsPage = useQuery({
    queryKey: ["runs", { workflow_name: name, limit: PAGE_SIZE + 1, offset }],
    queryFn: () => api.runs({ workflow_name: name, limit: PAGE_SIZE + 1, offset }),
    enabled: tab === "runs",
  });
  const runs = runsPage.data?.slice(0, PAGE_SIZE);
  const hasOlder = tab === "grid" ? !!grid.data?.has_more : (runsPage.data?.length ?? 0) > PAGE_SIZE;
  const shown = tab === "grid" ? (grid.data?.runs.length ?? 0) : (runs?.length ?? 0);
  const latest = useQuery({
    queryKey: ["runs", { workflow_name: name, limit: 1 }],
    queryFn: () => api.runs({ workflow_name: name, limit: 1 }),
    enabled: tab === "graph",
  });
  const goTo = (o: number) => setParams({ tab, ...(o > 0 ? { offset: String(o) } : {}) });
  const pager = (
    <Pager offset={offset} count={shown} hasOlder={hasOlder} onNewer={() => goTo(Math.max(0, offset - PAGE_SIZE))} onOlder={() => goTo(offset + PAGE_SIZE)} />
  );

  if (wf.isPending) return <Loading />;
  if (wf.error) return <ErrorBox error={wf.error} />;
  const w = wf.data;

  return (
    <div className="space-y-4">
      <div className="flex items-start justify-between">
        <div>
          <div className="text-xs text-slate-500">
            <Link to="/" className="hover:underline">
              Workflows
            </Link>{" "}
            /
          </div>
          <h1 className="text-xl font-semibold">
            {w.name} <span className="text-sm font-normal text-slate-400">v{w.version}</span>
          </h1>
        </div>
        <div className="flex gap-2">
          <RefreshButton />
          <Button variant="primary" onClick={() => setTriggering(true)} disabled={!w.entry_consumer} title={w.entry_consumer ? "" : "No entry_consumer"}>
            ▶ Trigger
          </Button>
        </div>
      </div>

      <div className="flex gap-1 border-b border-slate-200">
        {TABS.map((t) => (
          <button
            key={t}
            onClick={() => setParams({ tab: t })}
            className={`-mb-px border-b-2 px-4 py-2 text-sm font-medium capitalize ${
              tab === t ? "border-sky-600 text-sky-700" : "border-transparent text-slate-500 hover:text-slate-800"
            }`}
          >
            {t}
          </button>
        ))}
      </div>

      <ErrorBox error={grid.error ?? runsPage.error ?? latest.error} />
      {tab === "grid" && <GridView workflow={w} runs={grid.data?.runs} pager={pager} />}
      {tab === "graph" && <GraphView workflow={w} latestRun={latest.data?.[0]} />}
      {tab === "runs" && (
        <Card actions={pager}>{runs?.length ? <RunsTable runs={runs} /> : runsPage.isPending ? <Loading /> : <Empty>No runs yet.</Empty>}</Card>
      )}
      {tab === "details" && <DetailsView workflow={w} />}

      {triggering && <TriggerDialog workflow={w.name} onClose={() => setTriggering(false)} />}
    </div>
  );
}

/** Airflow-style grid: runs as columns (oldest → newest), jobs as rows. */
function Pager({ offset, count, hasOlder, onNewer, onOlder }: { offset: number; count: number; hasOlder: boolean; onNewer: () => void; onOlder: () => void }) {
  return (
    <div className="flex items-center gap-2 text-xs font-normal text-slate-500">
      {count > 0 && (
        <span>
          runs {offset + 1}–{offset + count}
        </span>
      )}
      <Button onClick={onNewer} disabled={offset === 0} title="More recent runs">
        ‹ Newer
      </Button>
      <Button onClick={onOlder} disabled={!hasOlder} title="Older runs">
        Older ›
      </Button>
    </div>
  );
}

function GridView({ workflow, runs, pager }: { workflow: WorkflowSummary; runs?: GridRun[]; pager: ReactNode }) {
  const navigate = useNavigate();
  const cols = [...(runs ?? [])].reverse();

  if (!runs) return <Loading />;
  if (!runs.length)
    return (
      <Card actions={pager}>
        <Empty>No runs on this page — trigger one, or go back to newer runs.</Empty>
      </Card>
    );

  return (
    <Card title={`Runs (oldest → newest)`} actions={pager}>
      <div className="overflow-x-auto">
        <table>
          <thead>
            <tr>
              <th className="pr-4" />
              {cols.map((r) => (
                <th key={r.id} className="px-0.5 align-bottom">
                  <Link to={`/runs/${r.id}`} title={`${shortId(r.id)} · ${r.status} · ${fmtTime(r.created_at)}`}>
                    <div className={`mx-auto h-10 w-3.5 rounded-sm ${styleFor(r.status).cell} hover:opacity-75`} />
                  </Link>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {workflow.jobs.map((job) => (
              <tr key={job.name}>
                <td className="whitespace-nowrap py-0.5 pr-4 text-sm text-slate-700">{job.name}</td>
                {cols.map((r) => {
                  const t = r.tasks.find((x) => x.task_name === job.name);
                  return (
                    <td key={r.id} className="px-0.5 py-0.5">
                      {t ? (
                        <button
                          title={`${job.name} · ${t.status} · attempt ${t.attempt}`}
                          onClick={() => navigate(`/runs/${r.id}?task=${t.id}`)}
                          className={`block size-3.5 rounded-sm ${styleFor(t.status).cell} hover:ring-2 hover:ring-sky-400`}
                        />
                      ) : (
                        <div className="size-3.5 rounded-sm border border-slate-200" />
                      )}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Legend />
    </Card>
  );
}

function Legend() {
  return (
    <div className="mt-4 flex flex-wrap gap-4 text-xs text-slate-500">
      {["SUCCEEDED", "RUNNING", "UP_FOR_RETRY", "FAILED", "PENDING", "CANCELLED"].map((s) => (
        <span key={s} className="flex items-center gap-1.5">
          <StatusDot status={s} /> {s.toLowerCase().replace(/_/g, " ")}
        </span>
      ))}
    </div>
  );
}

function GraphView({ workflow, latestRun }: { workflow: WorkflowSummary; latestRun?: WorkflowRunSummary }) {
  const tasks = useQuery({
    queryKey: ["tasks", latestRun?.id],
    queryFn: () => api.tasks(latestRun!.id),
    enabled: !!latestRun,
  });
  const latest = tasks.data ? latestAttempts(tasks.data) : {};
  const statusByTask = Object.fromEntries(Object.entries(latest).map(([k, v]) => [k, v.status]));
  return (
    <Card
      title={
        latestRun ? (
          <>
            Latest run{" "}
            <Link to={`/runs/${latestRun.id}`} className="font-mono text-sky-700 hover:underline">
              {shortId(latestRun.id)}
            </Link>
          </>
        ) : (
          "Graph"
        )
      }
    >
      <WorkflowGraph jobs={workflow.jobs} statusByTask={statusByTask} path={tasks.data ? executionPath(tasks.data) : undefined} />
      <Legend />
    </Card>
  );
}

function DetailsView({ workflow }: { workflow: WorkflowSummary }) {
  return (
    <Card title="Jobs">
      <table className="w-full">
        <thead className="border-b border-slate-200">
          <tr>
            <Th>Job</Th>
            <Th>Handler</Th>
            <Th>Input topic</Th>
            <Th>Routes to</Th>
            <Th>Retries</Th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {workflow.jobs.map((j) => (
            <tr key={j.name}>
              <Td className="font-medium">
                {j.name}
                {j.is_entry && <span className="ml-2 rounded bg-sky-100 px-1.5 py-0.5 text-[10px] font-semibold uppercase text-sky-700">entry</span>}
              </Td>
              <Td className="font-mono text-xs">{j.handler}</Td>
              <Td className="font-mono text-xs">{j.input_topic}</Td>
              <Td className="font-mono text-xs">
                {j.producer_routes.length ? (
                  j.producer_routes.map((p, i) => (
                    <div key={p}>
                      {p} <span className="text-slate-400">→ {j.output_topics[i]}</span>
                    </div>
                  ))
                ) : (
                  <span className="text-slate-400">terminal</span>
                )}
              </Td>
              <Td>
                {j.retries}
                {j.retries > 0 && <span className="text-xs text-slate-400"> × {j.retry_delay_seconds}s</span>}
              </Td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}
