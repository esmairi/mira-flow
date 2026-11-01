import { useQuery } from "@tanstack/react-query";
import { NavLink, Outlet } from "react-router-dom";
import { api } from "../lib/api";

const navCls = ({ isActive }: { isActive: boolean }) =>
  `rounded px-3 py-1.5 text-sm font-medium ${isActive ? "bg-white/15 text-white" : "text-sky-100 hover:text-white"}`;

export function Layout() {
  const ready = useQuery({ queryKey: ["ready"], queryFn: api.ready, retry: false });
  const ok = ready.isSuccess;
  return (
    <div className="min-h-screen">
      <nav className="bg-sky-900 shadow">
        <div className="mx-auto flex max-w-7xl items-center gap-6 px-4 py-2.5">
          <NavLink to="/" className="flex items-center gap-2 font-semibold text-white">
            <span className="grid size-7 place-items-center rounded bg-sky-500 text-xs font-bold">MF</span>
            Mira Flow
          </NavLink>
          <div className="flex gap-1">
            <NavLink to="/" end className={navCls}>
              Workflows
            </NavLink>
            <NavLink to="/runs" className={navCls}>
              Runs
            </NavLink>
          </div>
          <div className="ml-auto flex items-center gap-2 text-xs text-sky-100" title={ready.error?.message ?? "API + DB reachable"}>
            <span className={`size-2 rounded-full ${ready.isPending ? "bg-slate-400" : ok ? "bg-green-400" : "bg-red-500"}`} />
            {ready.isPending ? "checking…" : ok ? "api ready" : "api unreachable"}
          </div>
        </div>
      </nav>
      <main className="mx-auto max-w-7xl px-4 py-6">
        <Outlet />
      </main>
    </div>
  );
}
