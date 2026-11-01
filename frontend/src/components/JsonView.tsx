import { useEffect, useState } from "react";

export function JsonView({ label, value }: { label: string; value: unknown }) {
  const empty = value === null || value === undefined;
  const text = empty ? "" : JSON.stringify(value, null, 2);
  return (
    <div>
      <div className="mb-1 text-xs font-semibold uppercase tracking-wide text-slate-500">{label}</div>
      {empty ? (
        <div className="text-sm text-slate-400">—</div>
      ) : (
        <div className="group relative">
          <pre className="max-h-80 overflow-auto rounded-md bg-slate-900 p-3 pr-10 font-mono text-xs leading-relaxed text-slate-100">
            {text}
          </pre>
          <CopyButton text={text} label={label} />
        </div>
      )}
    </div>
  );
}

/** Copies `text` to the clipboard. `navigator.clipboard` only exists in
 *  secure contexts (https / localhost) — over plain http on a LAN IP it
 *  falls back to a hidden textarea + execCommand. */
async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard && window.isSecureContext) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the legacy path */
  }
  const el = document.createElement("textarea");
  el.value = text;
  el.setAttribute("readonly", "");
  el.style.position = "fixed";
  el.style.opacity = "0";
  document.body.appendChild(el);
  el.select();
  try {
    return document.execCommand("copy");
  } catch {
    return false;
  } finally {
    document.body.removeChild(el);
  }
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [state, setState] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    if (state === "idle") return;
    const t = setTimeout(() => setState("idle"), 1500);
    return () => clearTimeout(t);
  }, [state]);

  const title = state === "copied" ? "Copied!" : state === "failed" ? "Copy failed" : `Copy ${label.toLowerCase()}`;
  return (
    <button
      type="button"
      onClick={async () => setState((await copyText(text)) ? "copied" : "failed")}
      title={title}
      aria-label={title}
      className={`absolute right-2 top-2 rounded-md p-1.5 transition ${
        state === "copied"
          ? "bg-green-600 text-white"
          : state === "failed"
            ? "bg-red-600 text-white"
            : "bg-slate-700/80 text-slate-300 opacity-70 hover:bg-slate-600 hover:text-white group-hover:opacity-100"
      }`}
    >
      {state === "copied" ? (
        <svg viewBox="0 0 20 20" fill="currentColor" className="size-4" aria-hidden="true">
          <path
            fillRule="evenodd"
            d="M16.7 5.3a1 1 0 0 1 0 1.4l-8 8a1 1 0 0 1-1.4 0l-4-4a1 1 0 1 1 1.4-1.4L8 12.58l7.3-7.3a1 1 0 0 1 1.4 0Z"
            clipRule="evenodd"
          />
        </svg>
      ) : (
        <svg viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth={1.6} className="size-4" aria-hidden="true">
          <rect x="7" y="7" width="10" height="10" rx="2" />
          <path d="M13 7V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v6a2 2 0 0 0 2 2h2" />
        </svg>
      )}
    </button>
  );
}
