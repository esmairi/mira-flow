// Airflow-ish palette: green success, red failed, lime running,
// amber up_for_retry, gray pending, slate cancelled.
export const STATUS_STYLE: Record<string, { dot: string; badge: string; cell: string; fill: string }> = {
  SUCCEEDED: { dot: "bg-green-600", badge: "bg-green-100 text-green-800 ring-green-600/30", cell: "bg-green-600", fill: "fill-green-600" },
  FAILED: { dot: "bg-red-600", badge: "bg-red-100 text-red-800 ring-red-600/30", cell: "bg-red-600", fill: "fill-red-600" },
  RUNNING: { dot: "bg-lime-400", badge: "bg-lime-100 text-lime-800 ring-lime-600/30", cell: "bg-lime-400", fill: "fill-lime-400" },
  UP_FOR_RETRY: { dot: "bg-amber-400", badge: "bg-amber-100 text-amber-800 ring-amber-600/30", cell: "bg-amber-400", fill: "fill-amber-400" },
  PENDING: { dot: "bg-slate-300", badge: "bg-slate-100 text-slate-700 ring-slate-500/30", cell: "bg-slate-300", fill: "fill-slate-300" },
  CANCELLED: { dot: "bg-pink-400", badge: "bg-pink-100 text-pink-800 ring-pink-600/30", cell: "bg-pink-400", fill: "fill-pink-400" },
};

export const RUN_STATUSES = ["PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"] as const;

export const styleFor = (s: string) => STATUS_STYLE[s] ?? STATUS_STYLE.PENDING;
export const isTerminal = (s: string) => s === "SUCCEEDED" || s === "FAILED" || s === "CANCELLED";
