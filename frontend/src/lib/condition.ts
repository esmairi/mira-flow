import type { Condition } from "./types";

const OPS: Record<string, string> = {
  eq: "==",
  ne: "!=",
  gt: ">",
  gte: "≥",
  lt: "<",
  lte: "≤",
  in: "in",
  contains: "contains",
};

/** Human-readable form of a route's `when`, e.g. `score ≥ 50 AND score < 90`. */
export function formatCondition(c: Condition, nested = false): string {
  if ("all" in c || "any" in c) {
    const parts = "all" in c ? c.all : c.any;
    const text = parts.map((p) => formatCondition(p, true)).join("all" in c ? " AND " : " OR ");
    return nested && parts.length > 1 ? `(${text})` : text;
  }
  return `${c.field} ${OPS[c.op] ?? c.op} ${JSON.stringify(c.value)}`;
}
