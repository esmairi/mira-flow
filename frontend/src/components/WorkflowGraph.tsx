import dagre from "@dagrejs/dagre";
import {
  BaseEdge,
  Controls,
  EdgeLabelRenderer,
  Handle,
  MarkerType,
  Position,
  ReactFlow,
  type Edge as FlowEdge,
  type EdgeProps,
  type Node as FlowNode,
  type NodeProps,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { type CSSProperties, useLayoutEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";
import { formatCondition } from "../lib/condition";
import { styleFor } from "../lib/status";
import type { Condition, JobSummary } from "../lib/types";

const NODE_W = 190;
const NODE_H = 54;
const PILL_H = 18;
const CHAR_W = 6.2; // ~width of one 11px character
const PILL_MAX_CHARS = 18;

const COLORS = {
  taken: "#0284c7", // sky-600
  hover: "#334155", // slate-700
  normal: "#94a3b8", // slate-400
  faded: "#cbd5e1", // slate-300
};

// -- data -------------------------------------------------------------------

interface Route {
  from: string;
  to: string; // job name, or `topic:<name>` for a topic no job here consumes
  topic: string;
  producer: string;
  when: Condition | null;
  index: number; // priority within its job (0 = tried first)
  count: number;
  exits: boolean;
}

type JobNodeData = { label: string; sub: string; kind: "job" | "sink"; status?: string; clickable: boolean };
type RouteEdgeData = {
  route: Route;
  text: string;
  points: { x: number; y: number }[];
  label: { x: number; y: number };
  taken: boolean;
  faded: boolean;
  hovered: boolean;
  onHover: (id: string | null, rect?: DOMRect) => void;
};
type JobNode = FlowNode<JobNodeData, "job">;
type RouteEdge = FlowEdge<RouteEdgeData, "route">;

function collectRoutes(jobs: JobSummary[]): Route[] {
  const consumers = new Map<string, string[]>();
  for (const j of jobs) consumers.set(j.input_topic, [...(consumers.get(j.input_topic) ?? []), j.name]);
  return jobs.flatMap((j) => {
    const routes = j.routes ?? j.output_topics.map((topic, i) => ({ name: j.producer_routes[i], topic, when: null }));
    return routes.flatMap((r, index): Route[] => {
      const base = { from: j.name, topic: r.topic, producer: r.name, when: r.when, index, count: routes.length };
      const targets = consumers.get(r.topic);
      return targets
        ? targets.map((to) => ({ ...base, to, exits: false }))
        : [{ ...base, to: `topic:${r.topic}`, exits: true }];
    });
  });
}

/** Longest prefix shared by every topic, cut at a separator — `ex-order-` in
 *  `ex-order-payment`/`ex-order-notify` — so pills can show just the distinct part. */
function commonTopicPrefix(topics: string[]): string {
  if (topics.length < 2) return "";
  let prefix = topics[0];
  for (const t of topics) while (!t.startsWith(prefix)) prefix = prefix.slice(0, -1);
  const cut = Math.max(prefix.lastIndexOf("-"), prefix.lastIndexOf("."), prefix.lastIndexOf("_"));
  return cut >= 0 && topics.every((t) => t.length > cut + 1) ? prefix.slice(0, cut + 1) : "";
}

function describeCondition(r: Route): string {
  if (r.when) return formatCondition(r.when);
  return r.count > 1 ? "otherwise (default route)" : "always";
}

/** Dagre lays out nodes left→right and routes every edge around them, also
 *  reserving room for its label so pills never overlap nodes or each other. */
function layout(jobs: JobSummary[]) {
  const routes = collectRoutes(jobs);
  const prefix = commonTopicPrefix([...new Set([...jobs.map((j) => j.input_topic), ...routes.map((r) => r.topic)])]);
  const pillText = (topic: string) => {
    const short = topic.startsWith(prefix) ? topic.slice(prefix.length) : topic;
    return short.length > PILL_MAX_CHARS ? short.slice(0, PILL_MAX_CHARS - 1) + "…" : short;
  };

  const g = new dagre.graphlib.Graph({ multigraph: true });
  g.setGraph({ rankdir: "LR", nodesep: 28, ranksep: 70, edgesep: 18, marginx: 12, marginy: 12 });
  g.setDefaultEdgeLabel(() => ({}));
  for (const j of jobs) g.setNode(j.name, { width: NODE_W, height: NODE_H });
  for (const r of routes) {
    if (r.exits && !g.hasNode(r.to)) g.setNode(r.to, { width: NODE_W, height: NODE_H });
    const text = pillText(r.topic);
    g.setEdge(r.from, r.to, { width: text.length * CHAR_W + 14, height: PILL_H, labelpos: "c" }, `${r.index}`);
  }
  dagre.layout(g);

  const graph = g.graph();
  return {
    routes,
    pillText,
    node: (id: string) => g.node(id),
    edge: (r: Route) => g.edge({ v: r.from, w: r.to, name: `${r.index}` }),
    width: graph.width ?? 0,
    height: graph.height ?? 0,
  };
}

// -- rendering --------------------------------------------------------------

function JobNodeView({ data }: NodeProps<JobNode>) {
  const job = data.kind === "job";
  return (
    <div
      className={`flex h-full w-full overflow-hidden rounded-md border bg-white text-left shadow-sm ${
        job ? "border-slate-300" : "border-dashed border-slate-300 bg-slate-50"
      } ${data.clickable ? "cursor-pointer hover:border-sky-400" : ""}`}
      title={job ? `${data.label}\nhandler: ${data.sub}${data.status ? `\nstatus: ${data.status}` : ""}` : `topic ${data.label} — no job in this workflow consumes it`}
    >
      <Handle type="target" position={Position.Left} className="!pointer-events-none !opacity-0" />
      {job && <div className={`w-1.5 shrink-0 ${data.status ? styleFor(data.status).cell : "bg-slate-200"}`} />}
      <div className="min-w-0 px-3 py-2">
        <div className={`truncate text-[13px] font-semibold ${job ? "text-slate-800" : "text-slate-600"}`}>{data.label}</div>
        <div className="truncate text-[11px] text-slate-500">{data.sub}</div>
      </div>
      <Handle type="source" position={Position.Right} className="!pointer-events-none !opacity-0" />
    </div>
  );
}

/** Smooth path through dagre's routing points, with horizontal tangents at
 *  each one (the graph flows left→right). */
function smoothPath(points: { x: number; y: number }[]): string {
  let d = `M${points[0].x},${points[0].y}`;
  for (let i = 1; i < points.length; i++) {
    const a = points[i - 1];
    const b = points[i];
    const dx = (b.x - a.x) / 2;
    d += ` C${a.x + dx},${a.y} ${b.x - dx},${b.y} ${b.x},${b.y}`;
  }
  return d;
}

function RouteEdgeView({ id, sourceX, sourceY, targetX, targetY, markerEnd, data }: EdgeProps<RouteEdge>) {
  if (!data) return null;
  // Dagre's interior points, re-anchored on React Flow's actual handles.
  const pts = [{ x: sourceX, y: sourceY }, ...data.points.slice(1, -1), { x: targetX, y: targetY }];
  const color = data.hovered ? COLORS.hover : data.taken ? COLORS.taken : data.faded ? COLORS.faded : COLORS.normal;
  const width = data.hovered || data.taken ? 2.5 : data.faded ? 1.25 : 1.5;
  const pill = data.hovered
    ? "border-slate-800 bg-slate-800 text-white"
    : data.taken
      ? "border-sky-500 bg-sky-50 font-medium text-sky-800"
      : "border-slate-300 bg-white text-slate-600";
  return (
    <>
      <BaseEdge id={id} path={smoothPath(pts)} markerEnd={markerEnd} style={{ stroke: color, strokeWidth: width }} />
      <EdgeLabelRenderer>
        <div
          className={`nodrag nopan absolute cursor-help whitespace-nowrap rounded-full border px-2 text-[11px] leading-4 ${pill}`}
          style={{ transform: `translate(-50%, -50%) translate(${data.label.x}px, ${data.label.y}px)`, pointerEvents: "all", zIndex: data.hovered ? 2 : data.taken ? 1 : 0 }}
          onMouseEnter={(e) => data.onHover(id, e.currentTarget.getBoundingClientRect())}
          onMouseLeave={() => data.onHover(null)}
        >
          {data.text}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

const rank = (e: RouteEdge) => (e.data?.hovered ? 2 : e.data?.taken ? 1 : 0);

const nodeTypes = { job: JobNodeView };
const edgeTypes = { route: RouteEdgeView };

export function WorkflowGraph({
  jobs,
  statusByTask,
  path,
  onSelect,
}: {
  jobs: JobSummary[];
  statusByTask?: Record<string, string>;
  /** Task names in the order a run executed them — those edges are highlighted. */
  path?: string[];
  onSelect?: (task: string) => void;
}) {
  const [hover, setHover] = useState<{ id: string; rect: DOMRect } | null>(null);
  const graph = useMemo(() => layout(jobs), [jobs]);
  const pathKey = (path ?? []).join("\u0000");
  const taken = useMemo(() => {
    const p = pathKey ? pathKey.split("\u0000") : [];
    return new Set(p.slice(1).map((to, i) => `${p[i]}->${to}`));
  }, [pathKey]);
  const hasPath = taken.size > 0;
  const statusKey = JSON.stringify(statusByTask ?? {});

  const nodes: JobNode[] = useMemo(() => {
    const status: Record<string, string> = JSON.parse(statusKey);
    const ids = [...new Set([...jobs.map((j) => j.name), ...graph.routes.filter((r) => r.exits).map((r) => r.to)])];
    return ids.map((id) => {
      const n = graph.node(id);
      const job = jobs.find((j) => j.name === id);
      return {
        id,
        type: "job",
        position: { x: n.x - NODE_W / 2, y: n.y - NODE_H / 2 },
        width: NODE_W,
        height: NODE_H,
        draggable: false,
        selectable: false,
        connectable: false,
        data: job
          ? { label: job.name, sub: job.handler, kind: "job", status: status[id], clickable: !!onSelect }
          : { label: id.slice("topic:".length), sub: "leaves the workflow", kind: "sink", clickable: false },
      };
    });
  }, [graph, jobs, statusKey, onSelect]);

  const edges: RouteEdge[] = useMemo(
    () =>
      graph.routes.map((r): RouteEdge => {
        const id = `${r.from}->${r.to}#${r.index}`;
        const isTaken = taken.has(`${r.from}->${r.to}`);
        const hovered = hover?.id === id;
        const e = graph.edge(r);
        const color = hovered ? COLORS.hover : isTaken ? COLORS.taken : hasPath ? COLORS.faded : COLORS.normal;
        return {
          id,
          source: r.from,
          target: r.to,
          type: "route",
          markerEnd: { type: MarkerType.ArrowClosed, color, width: 16, height: 16 },
          data: {
            route: r,
            text: graph.pillText(r.topic),
            points: e.points ?? [],
            label: { x: e.x ?? 0, y: e.y ?? 0 },
            taken: isTaken,
            faded: hasPath && !isTaken,
            hovered,
            onHover: (hid, rect) => setHover(hid && rect ? { id: hid, rect } : null),
          },
        };
      })
        // Stacking by array order, not zIndex: an edge with a zIndex gets its
        // own layer above the label layer and would cross out the pills.
        .sort((a, b) => rank(a) - rank(b)),
    [graph, taken, hasPath, hover],
  );

  if (!jobs.length) return null;
  const tip = hover ? edges.find((e) => e.id === hover.id) : undefined;

  return (
    <div className="rounded-md border border-slate-200 bg-slate-50/60" style={{ height: Math.min(560, Math.max(220, graph.height + 40)) }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        onNodeClick={(_, n) => n.data.kind === "job" && onSelect?.(n.id)}
        onMoveStart={() => setHover(null)}
        fitView
        fitViewOptions={{ padding: 0.08, maxZoom: 1, minZoom: 0.4 }}
        minZoom={0.3}
        maxZoom={1.5}
        nodesDraggable={false}
        nodesConnectable={false}
        elementsSelectable={false}
        zoomOnScroll
        zoomOnDoubleClick
        proOptions={{ hideAttribution: true }}
      >
        <Controls showInteractive={false} />
      </ReactFlow>
      {tip?.data && hover && <RouteTooltip anchor={hover.rect} route={tip.data.route} taken={tip.data.taken} />}
    </div>
  );
}

/** Fixed-position tooltip rendered on <body>, so the graph's container can't
 *  clip it; flips above the pill when there's no room below, and is clamped
 *  horizontally inside the viewport. */
function RouteTooltip({ anchor, route: r, taken }: { anchor: DOMRect; route: Route; taken: boolean }) {
  const ref = useRef<HTMLDivElement>(null);
  const [style, setStyle] = useState<CSSProperties>({ visibility: "hidden", left: 0, top: 0 });

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const { width, height } = el.getBoundingClientRect();
    const margin = 8;
    const left = Math.min(Math.max(anchor.left + anchor.width / 2 - width / 2, margin), window.innerWidth - width - margin);
    const below = anchor.bottom + 6;
    const top = below + height + margin <= window.innerHeight ? below : Math.max(margin, anchor.top - height - 6);
    setStyle({ left, top, visibility: "visible" });
  }, [anchor]);

  // Blue when this route is on the run's path, gray otherwise.
  const t = taken
    ? { box: "bg-sky-700 ring-sky-500", muted: "text-sky-200", body: "text-sky-100", strong: "text-white", cond: "text-amber-200" }
    : { box: "bg-slate-600 ring-slate-500", muted: "text-slate-300", body: "text-slate-200", strong: "text-white", cond: "text-amber-200" };

  return createPortal(
    <div
      ref={ref}
      style={style}
      className={`pointer-events-none fixed z-50 w-max max-w-xs rounded-md px-3 py-2 text-xs shadow-lg ring-1 ${t.box} ${t.body}`}
    >
      <div className={`mb-1 text-[10px] uppercase tracking-wide ${t.muted}`}>
        {r.from} → {r.exits ? "outside the workflow" : r.to}
      </div>
      <div className={`font-mono text-sm ${t.cond}`}>{describeCondition(r)}</div>
      <div className="mt-1.5 space-y-0.5">
        <div>
          topic <span className={`font-mono ${t.strong}`}>{r.topic}</span>
        </div>
        <div>
          producer <span className={`font-mono ${t.strong}`}>{r.producer}</span>
        </div>
        {r.count > 1 && (
          <div>
            route {r.index + 1} of {r.count} — first match wins
          </div>
        )}
        {r.exits && <div className={t.muted}>no job here consumes it: the run ends after this hand-off</div>}
        {taken && <div className={`font-medium ${t.strong}`}>✓ taken by this run</div>}
      </div>
    </div>,
    document.body,
  );
}
