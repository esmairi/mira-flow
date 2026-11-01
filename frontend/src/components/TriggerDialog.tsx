import { useMutation, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { api } from "../lib/api";
import { Button, ErrorBox, Modal } from "./ui";

export function TriggerDialog({ workflow, onClose }: { workflow: string; onClose: () => void }) {
  const [key, setKey] = useState<string>(newKey);
  const [input, setInput] = useState("{\n  \n}");
  const [parseError, setParseError] = useState<string | null>(null);
  const qc = useQueryClient();
  const navigate = useNavigate();

  const trigger = useMutation({
    mutationFn: (payload: Record<string, unknown>) => api.trigger(workflow, key, payload),
    onSuccess: (res) => {
      qc.invalidateQueries({ queryKey: ["runs"] });
      qc.invalidateQueries({ queryKey: ["grid"] });
      navigate(`/runs/${res.workflow_run_id}`);
    },
  });

  const submit = () => {
    let payload: unknown;
    try {
      payload = input.trim() ? JSON.parse(input) : {};
    } catch (e) {
      setParseError(`Invalid JSON: ${(e as Error).message}`);
      return;
    }
    if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
      setParseError("Input must be a JSON object");
      return;
    }
    setParseError(null);
    trigger.mutate(payload as Record<string, unknown>);
  };

  return (
    <Modal title={`Trigger ${workflow}`} onClose={onClose}>
      <div className="space-y-4">
        <label className="block">
          <span className="text-sm font-medium text-slate-700">Idempotency key</span>
          <input
            value={key}
            onChange={(e) => setKey(e.target.value)}
            className="mt-1 w-full rounded-md border border-slate-300 px-3 py-1.5 font-mono text-sm focus:border-sky-500 focus:outline-none"
          />
          <span className="text-xs text-slate-500">Re-using a key returns the existing run instead of starting a new one.</span>
        </label>
        <label className="block">
          <span className="text-sm font-medium text-slate-700">Input (JSON)</span>
          <textarea
            value={input}
            onChange={(e) => setInput(e.target.value)}
            rows={10}
            spellCheck={false}
            className="mt-1 w-full rounded-md border border-slate-300 px-3 py-2 font-mono text-sm focus:border-sky-500 focus:outline-none"
          />
        </label>
        <ErrorBox error={parseError ?? trigger.error} />
        <div className="flex justify-end gap-2">
          <Button onClick={onClose}>Cancel</Button>
          <Button variant="primary" onClick={submit} disabled={trigger.isPending || !key.trim()}>
            {trigger.isPending ? "Triggering…" : "▶ Trigger"}
          </Button>
        </div>
      </div>
    </Modal>
  );
}

// crypto.randomUUID is only available in secure contexts (https / localhost).
function newKey(): string {
  return typeof crypto.randomUUID === "function"
    ? crypto.randomUUID()
    : `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
