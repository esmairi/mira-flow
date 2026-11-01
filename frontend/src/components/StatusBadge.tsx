import { styleFor } from "../lib/status";

export function StatusBadge({ status }: { status: string }) {
  return (
    <span
      className={`inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium ring-1 ring-inset ${styleFor(status).badge}`}
    >
      <span className={`size-1.5 rounded-full ${styleFor(status).dot}`} />
      {status.toLowerCase().replace(/_/g, " ")}
    </span>
  );
}

export function StatusDot({ status, title }: { status: string; title?: string }) {
  return <span title={title ?? status} className={`inline-block size-3 rounded-full ${styleFor(status).dot}`} />;
}
