"""Panel web de SlurmScope (solo biblioteca estándar).

``slurmscope serve`` publica el estado de uno o varios barridos en un navegador:
avance, estimación de tiempos, historial de envíos, ejecuciones con su estado y
duración, métricas agregadas, fallos y cola del clúster. Es de **solo lectura**: no
envía ni cancela nada, así que se puede exponer sin dar control sobre el clúster.

La sincronización con el clúster se hace en un hilo aparte cada ``sync_interval``
segundos; las peticiones HTTP sirven la última fotografía y nunca esperan al SSH.
"""

from __future__ import annotations

import hashlib
import json
import os
import random
import shutil
import sys
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence
from urllib.parse import urlparse

from . import __version__
from .backends import BaseBackend, MockSlurm, get_backend
from .config import ConfigError, SweepSpec, load_spec
from .ledger import COMPLETED, FAILED, Ledger
from .matrix import expand_runs
from .report import build_dashboard, dashboard_payload
from .sync import sync_states

# Un proyecto de demostración solo se reinicia (borrando su libro y sus resultados)
# si contiene este fichero: así el modo demo nunca toca un barrido real.
DEMO_MARKER = ".slurmscope-demo"
REMOTE_MIN_INTERVAL = 120.0


class Project:
    """Un barrido servido por el panel, con su última fotografía en memoria."""

    def __init__(self, directory: Path, backend_name: str = "auto"):
        self.directory = directory.resolve()
        self.spec: SweepSpec = load_spec(self.directory)
        state = self.directory / ".slurmscope"
        state.mkdir(parents=True, exist_ok=True)
        self.ledger = Ledger(state / "ledger.db")
        self.is_demo = (self.directory / DEMO_MARKER).is_file()
        if self.is_demo:
            backend_name = "mock"  # una demostración nunca toca un clúster real
        self.backend: BaseBackend = get_backend(
            backend_name,
            mock_state=state / "mock_cluster.json",
            parallel=max(1, self.spec.slurm.max_parallel or 4),
            failure_rate=0.04,
            cluster=self.spec.cluster,
            project=self.spec.project,
            workdir=self.directory,
        )
        # Un clúster real no se consulta más de una vez cada REMOTE_MIN_INTERVAL
        # segundos, ni cuando no queda nada pendiente: el nodo de login es compartido.
        self.min_interval = 0.0 if isinstance(self.backend, MockSlurm) else REMOTE_MIN_INTERVAL
        self.lock = threading.Lock()
        self.snapshot: Dict[str, Any] = {}
        self.last_sync: Optional[float] = None
        self.last_error: Optional[str] = None
        self.cycle = 1
        self._idle_rounds = 0

    @property
    def slug(self) -> str:
        return self.directory.name

    # ── actualización ────────────────────────────────────────────────────────
    def has_active_runs(self) -> bool:
        counts = self.ledger.counts(project=self.spec.project)
        return bool(counts.get("PENDING") or counts.get("RUNNING"))

    def should_sync(self, now: float) -> bool:
        if self.last_sync is None:
            return True
        if not self.has_active_runs():
            return False
        return now - self.last_sync >= self.min_interval

    def refresh(self, sync: bool, demo_tick: float = 0.0) -> None:
        with self.lock:
            if sync and not isinstance(self.backend, MockSlurm):
                sync = self.should_sync(time.time())
            try:
                if demo_tick and isinstance(self.backend, MockSlurm):
                    self._demo_step(demo_tick)
                dash = build_dashboard(self.spec, self.ledger, backend=self.backend,
                                       sync=sync)
                payload = dashboard_payload(dash, self.ledger)
                payload["backend"] = self.backend.name
                payload["demo"] = self.is_demo and isinstance(self.backend, MockSlurm)
                payload["cycle"] = self.cycle
                payload["slug"] = self.slug
                payload["synced"] = sync
                self.snapshot = payload
                if sync or self.last_sync is None:
                    self.last_sync = time.time()
                self.last_error = None
            except Exception as exc:  # el panel no debe caerse por un barrido
                self.last_error = f"{type(exc).__name__}: {exc}"
            if self.snapshot:
                self.snapshot["last_sync"] = self.last_sync
                self.snapshot["last_error"] = self.last_error

    # ── modo demostración (solo simulador y con marcador) ────────────────────
    def _demo_step(self, tick: float) -> None:
        backend = self.backend
        assert isinstance(backend, MockSlurm)
        if not self.ledger.submissions(project=self.spec.project):
            self._demo_submit()
            return
        backend.advance(tick)
        sync_states(self.ledger, backend, project=self.spec.project)
        self._demo_write_results()
        if backend.pending_count() == 0:
            self._idle_rounds += 1
            if self._idle_rounds >= 6 and self.is_demo:
                self._demo_reset()
        else:
            self._idle_rounds = 0

    def _demo_submit(self) -> None:
        from .cli import _generate, submit_sweep

        runs = expand_runs(self.spec)
        artifacts = _generate(self.spec, runs, force=True)
        submit_sweep(self.spec, self.backend, self.ledger, runs, artifacts,
                     log=lambda _m: None)

    def _demo_write_results(self) -> None:
        """El simulador no ejecuta nada: escribe unas métricas plausibles para que el
        panel de resultados tenga contenido. Solo en proyectos marcados como demo."""
        if not self.is_demo:
            return
        root = self.spec.results_path()
        for record in self.ledger.runs(project=self.spec.project):
            if record.state not in (COMPLETED, FAILED) or not record.run_dir:
                continue
            run_dir = (self.directory / record.run_dir).resolve()
            if root not in run_dir.parents:
                continue
            marker = run_dir / ("FAILED" if record.state == FAILED else "metrics.json")
            if marker.exists():
                continue
            run_dir.mkdir(parents=True, exist_ok=True)
            if record.state == FAILED:
                marker.write_text("exit 1 (simulado)\n", encoding="utf-8")
                continue
            seed = int(hashlib.sha256(record.run_key.encode()).hexdigest()[:8], 16)
            rng = random.Random(seed)
            params = record.params or {}
            depth = float(params.get("depth", params.get("layers", 2)) or 2)
            qubits = float(params.get("qubits", 4) or 4)
            accuracy = min(0.99, 0.62 + 0.035 * qubits + 0.02 * depth + rng.gauss(0, 0.02))
            marker.write_text(json.dumps({
                "params": params,
                "metrics": {
                    "accuracy": round(accuracy, 4),
                    "f1": round(accuracy - abs(rng.gauss(0.02, 0.01)), 4),
                    "loss": round(max(0.02, 1.15 - accuracy + rng.gauss(0, 0.03)), 4),
                },
            }, indent=2), encoding="utf-8")

    def _demo_reset(self) -> None:
        state = self.directory / ".slurmscope"
        for name in ("ledger.db", "mock_cluster.json"):
            (state / name).unlink(missing_ok=True)
        results = self.spec.results_path()
        if results.is_dir() and self.directory in results.parents:
            shutil.rmtree(results)
        self.ledger = Ledger(state / "ledger.db")
        self.backend.reset()  # type: ignore[attr-defined]
        self.cycle += 1
        self._idle_rounds = 0


class Panel:
    """Conjunto de barridos y el hilo que los mantiene al día."""

    def __init__(self, projects: Sequence[Project], sync_interval: float,
                 demo_tick: float = 0.0):
        self.projects = list(projects)
        self.sync_interval = sync_interval
        self.demo_tick = demo_tick
        self.started = time.time()
        self._stop = threading.Event()

    def refresh_all(self, sync: bool = True) -> None:
        for project in self.projects:
            project.refresh(sync=sync, demo_tick=self.demo_tick if sync else 0.0)

    def loop(self) -> None:
        self.refresh_all(sync=True)
        while not self._stop.wait(self.sync_interval):
            self.refresh_all(sync=True)

    def start(self) -> None:
        # primera fotografía sin tocar el clúster (inmediata); la sincronización real
        # va en su hilo, para que un SSH lento no retrase el arranque del servidor
        self.refresh_all(sync=False)
        if self.sync_interval > 0:
            threading.Thread(target=self.loop, name="slurmscope-sync", daemon=True).start()

    def stop(self) -> None:
        self._stop.set()

    def summary(self) -> Dict[str, Any]:
        items = []
        for project in self.projects:
            snap = project.snapshot or {}
            items.append({
                "slug": project.slug,
                "project": snap.get("project", project.spec.project),
                "profile": project.spec.cluster.profile,
                "host": project.spec.cluster.host,
                "backend": project.backend.name,
                "demo": snap.get("demo", False),
                "progress": snap.get("progress", 0.0),
                "total": snap.get("total", 0),
                "counts": snap.get("counts", {}),
                "eta": (snap.get("estimate") or {}).get("eta"),
                "last_sync": project.last_sync,
                "last_error": project.last_error,
            })
        return {"version": __version__, "sync_interval": self.sync_interval,
                "started": self.started, "projects": items}

    def find(self, slug: str) -> Optional[Project]:
        for project in self.projects:
            if project.slug == slug:
                return project
        return None


def make_handler(panel: Panel):
    class Handler(BaseHTTPRequestHandler):
        server_version = f"SlurmScope/{__version__}"

        def log_message(self, fmt: str, *args: Any) -> None:
            if os.environ.get("SLURMSCOPE_ACCESS_LOG"):
                sys.stderr.write("[serve] " + (fmt % args) + "\n")

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'none'; script-src 'self' 'unsafe-inline'; "
                "style-src 'self' 'unsafe-inline'; connect-src 'self'; img-src 'self' data:",
            )
            self.end_headers()
            if self.command != "HEAD":
                self.wfile.write(body)

        def _json(self, payload: Any, status: int = 200) -> None:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
            self._send(status, body, "application/json; charset=utf-8")

        def do_HEAD(self) -> None:  # noqa: N802
            self.do_GET()

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path.rstrip("/") or "/"
            if path == "/":
                self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
                return
            if path == "/healthz":
                self._json({"ok": True, "version": __version__})
                return
            if path == "/api/projects":
                self._json(panel.summary())
                return
            parts = path.split("/")
            if len(parts) == 4 and parts[1] == "api" and parts[2] == "project":
                project = panel.find(parts[3])
                if project is None:
                    self._json({"error": "proyecto desconocido"}, HTTPStatus.NOT_FOUND)
                    return
                self._json(project.snapshot or {"error": project.last_error or "cargando"})
                return
            if len(parts) == 5 and parts[1] == "api" and parts[2] == "project" \
                    and parts[4] == "runs.csv":
                project = panel.find(parts[3])
                if project is None:
                    self._json({"error": "proyecto desconocido"}, HTTPStatus.NOT_FOUND)
                    return
                self._send(200, _runs_csv(project.snapshot).encode("utf-8"),
                           "text/csv; charset=utf-8")
                return
            self._json({"error": "no encontrado"}, HTTPStatus.NOT_FOUND)

    return Handler


def _runs_csv(snapshot: Dict[str, Any]) -> str:
    import csv
    import io

    runs = snapshot.get("runs") or []
    params = list((snapshot.get("sweep") or {}).keys())
    metric_names = sorted({k for r in runs for k in (r.get("metrics") or {})})
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["index", "key", *params, "state", "disk", "seconds", "exit_code",
                     *metric_names])
    for r in runs:
        writer.writerow([r.get("index"), r.get("key"),
                         *[(r.get("params") or {}).get(p) for p in params],
                         r.get("state"), r.get("disk"), r.get("seconds"),
                         r.get("exit_code"),
                         *[(r.get("metrics") or {}).get(m) for m in metric_names]])
    return buffer.getvalue()


def serve(directories: Sequence[Path], host: str = "127.0.0.1", port: int = 8120,
          sync_interval: float = 60.0, backend: str = "auto",
          demo_tick: float = 0.0) -> int:
    projects: List[Project] = []
    for directory in directories:
        try:
            projects.append(Project(Path(directory), backend_name=backend))
        except ConfigError as exc:
            print(f"error en {directory}: {exc}", file=sys.stderr)
            return 2
    slugs = [p.slug for p in projects]
    if len(set(slugs)) != len(slugs):
        print("error: dos proyectos con el mismo nombre de directorio", file=sys.stderr)
        return 2

    panel = Panel(projects, sync_interval=sync_interval, demo_tick=demo_tick)
    panel.start()
    server = ThreadingHTTPServer((host, port), make_handler(panel))
    server.daemon_threads = True
    print(f"SlurmScope {__version__} · panel en http://{host}:{port} · "
          f"{len(projects)} barrido(s) · sincroniza cada {sync_interval:g} s")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\ndetenido")
    finally:
        panel.stop()
        server.server_close()
    return 0


PAGE = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>SlurmScope</title>
<style>
:root{--bg:#0d1117;--card:#161b22;--line:#30363d;--fg:#e6edf3;--mute:#8b949e;
--acc:#58a6ff;--ok:#3fb950;--run:#d29922;--bad:#f85149;--pend:#8b949e;--chip:#21262d}
@media (prefers-color-scheme: light){:root{--bg:#f6f8fa;--card:#fff;--line:#d0d7de;
--fg:#1f2328;--mute:#59636e;--chip:#eaeef2}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif}
header{display:flex;align-items:center;gap:14px;padding:14px 22px;border-bottom:1px solid var(--line);
position:sticky;top:0;background:var(--bg);z-index:2;flex-wrap:wrap}
header h1{font-size:18px;margin:0;letter-spacing:.2px}
header h1 span{color:var(--acc)}
.mute{color:var(--mute)}
main{padding:18px 22px;max-width:1400px;margin:0 auto}
.tabs{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
.tab{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:10px 14px;
cursor:pointer;min-width:210px;text-align:left;color:var(--fg);font:inherit}
.tab.on{border-color:var(--acc);box-shadow:0 0 0 1px var(--acc) inset}
.tab b{display:block;font-size:14px}
.tab small{color:var(--mute)}
.mini{height:4px;background:var(--chip);border-radius:3px;margin-top:8px;overflow:hidden}
.mini i{display:block;height:100%;background:var(--ok)}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:12px 0}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:12px 14px}
.card .k{color:var(--mute);font-size:12px;text-transform:uppercase;letter-spacing:.4px}
.card .v{font-size:22px;font-weight:600;margin-top:2px}
.bar{display:flex;height:14px;border-radius:7px;overflow:hidden;background:var(--chip);margin:8px 0 4px}
.bar i{display:block;height:100%}
.legend{display:flex;gap:14px;flex-wrap:wrap;font-size:12px;color:var(--mute)}
.dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle}
section{margin-top:18px}
section h2{font-size:15px;margin:0 0 8px}
.two{display:grid;grid-template-columns:1fr 1fr;gap:14px}
@media (max-width:900px){.two{grid-template-columns:1fr}}
table{width:100%;border-collapse:collapse;background:var(--card);border:1px solid var(--line);
border-radius:10px;overflow:hidden;font-size:13px}
th,td{padding:7px 10px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--mute);font-weight:600;font-size:12px;background:var(--chip);position:sticky;top:0}
td.r,th.r{text-align:right;font-variant-numeric:tabular-nums}
tr:last-child td{border-bottom:none}
.scroll{max-height:460px;overflow:auto;border-radius:10px}
.st{padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}
.COMPLETED,.OK{background:rgba(63,185,80,.15);color:var(--ok)}
.RUNNING,.PARCIAL{background:rgba(210,153,34,.15);color:var(--run)}
.FAILED,.FALLO,.CANCELLED{background:rgba(248,81,73,.15);color:var(--bad)}
.PENDING,.PENDIENTE{background:var(--chip);color:var(--pend)}
.tools{display:flex;gap:8px;margin-bottom:8px;flex-wrap:wrap}
input,select{background:var(--card);color:var(--fg);border:1px solid var(--line);border-radius:8px;
padding:6px 10px;font:inherit}
a{color:var(--acc)}
.chip{display:inline-block;background:var(--chip);border-radius:6px;padding:1px 7px;margin:2px 4px 2px 0;font-size:12px}
.warn{background:rgba(210,153,34,.12);border:1px solid rgba(210,153,34,.4);border-radius:8px;
padding:8px 12px;margin:8px 0;color:var(--run)}
.demo{background:rgba(88,166,255,.12);border:1px solid rgba(88,166,255,.4);color:var(--acc);
border-radius:999px;padding:2px 10px;font-size:12px;font-weight:600}
.right{margin-left:auto}
footer{padding:20px 22px;color:var(--mute);font-size:12px;text-align:center}
</style>
</head>
<body>
<header>
  <h1>Slurm<span>Scope</span></h1>
  <span class="mute" id="sub">panel de barridos</span>
  <span class="right mute" id="clock"></span>
</header>
<main>
  <div class="tabs" id="tabs"></div>
  <div id="view"><p class="mute">Cargando…</p></div>
</main>
<footer>SlurmScope · panel de solo lectura · <span id="ver"></span> ·
<a href="https://github.com/danimap27/slurmscope">código fuente</a></footer>
<script>
"use strict";
const $ = (s, el=document) => el.querySelector(s);
const COLORS = {COMPLETED:"var(--ok)", RUNNING:"var(--run)", PENDING:"var(--pend)",
                FAILED:"var(--bad)", CANCELLED:"#a371f7"};
let current = null, summary = null, filter = {state:"", q:""};

function el(tag, attrs={}, ...kids){
  const n = document.createElement(tag);
  for (const [k,v] of Object.entries(attrs)){
    if (k === "class") n.className = v; else if (k === "style") n.style.cssText = v;
    else if (k.startsWith("on")) n.addEventListener(k.slice(2), v); else n.setAttribute(k, v);
  }
  for (const k of kids.flat()){ if (k == null) continue;
    n.append(k instanceof Node ? k : document.createTextNode(String(k))); }
  return n;
}
function fmtS(s){
  if (s == null || isNaN(s)) return "—";
  s = Math.round(s); const d = Math.floor(s/86400), h = Math.floor(s%86400/3600),
  m = Math.floor(s%3600/60), x = s%60;
  if (d) return `${d} d ${h} h`; if (h) return `${h} h ${m} min`;
  if (m) return `${m} min ${x} s`; return `${x} s`;
}
function ago(t){ if(!t) return "nunca"; const s = Date.now()/1000 - t;
  return s < 60 ? `hace ${Math.round(s)} s` : `hace ${Math.round(s/60)} min`; }
function badge(v){ return v ? el("span", {class:"st "+v}, v) : el("span", {class:"mute"}, "—"); }
function num(v, d=4){ return typeof v === "number" ? (Number.isInteger(v) ? v : v.toFixed(d)) : (v ?? "—"); }
function table(headers, rows, right=[]){
  return el("table", {}, el("thead", {}, el("tr", {}, headers.map((h,i)=>el("th",{class:right.includes(i)?"r":""},h)))),
    el("tbody", {}, rows.length ? rows.map(r => el("tr", {}, r.map((c,i)=>el("td",{class:right.includes(i)?"r":""},c))))
      : el("tr", {}, el("td", {colspan:headers.length, class:"mute"}, "Sin datos"))));
}

async function loadSummary(){
  const r = await fetch("api/projects"); summary = await r.json();
  $("#ver").textContent = "v" + summary.version;
  $("#sub").textContent = `${summary.projects.length} barrido(s) · sincroniza cada ${summary.sync_interval} s`;
  if (!current && summary.projects.length) current = summary.projects[0].slug;
  const tabs = $("#tabs"); tabs.replaceChildren();
  for (const p of summary.projects){
    const c = p.counts || {};
    tabs.append(el("button", {class:"tab"+(p.slug===current?" on":""), onclick:()=>{current=p.slug; refresh();}},
      el("b", {}, p.project),
      el("small", {}, `${p.profile}${p.host ? " · "+p.host : ""} · ${p.backend}`),
      el("div", {class:"mini"}, el("i", {style:`width:${p.progress}%`})),
      el("small", {}, `${(p.progress||0).toFixed(1)} % · ${c.COMPLETED||0}/${p.total}` +
        (p.last_error ? " · ⚠ error" : ""))));
  }
}

function renderProject(d){
  const v = $("#view"); v.replaceChildren();
  if (d.error){ v.append(el("div",{class:"warn"}, d.error)); return; }
  const c = d.counts || {}, e = d.estimate || {}, cl = d.cluster || {}, total = d.total || 0;
  const active = (c.RUNNING||0) + (c.PENDING||0), failed = (c.FAILED||0) + (c.CANCELLED||0);

  v.append(el("div", {style:"display:flex;gap:10px;align-items:center;flex-wrap:wrap"},
    el("h2", {style:"margin:0;font-size:20px"}, d.project),
    d.demo ? el("span", {class:"demo"}, `DEMO · clúster simulado · ciclo ${d.cycle}`) : null,
    el("span", {class:"mute"}, `actualizado ${ago(d.last_sync)}`)));
  v.append(el("div", {class:"mute", style:"margin-top:4px"},
    `perfil ${cl.profile}`, cl.host ? ` · ${cl.host}` : " · local", ` · backend ${d.backend}`,
    ` · partición ${cl.partition || "por defecto"}`, cl.account ? ` · cuenta ${cl.account}` : "",
    cl.qos ? ` · QoS ${cl.qos}` : "", ` · MaxArraySize ${cl.max_array_size}`,
    ` · ${d.slurm.cpus_per_task} CPU · ${d.slurm.mem} · ${d.slurm.time}`));
  for (const w of (d.warnings || [])) v.append(el("div", {class:"warn"}, "⚠ " + w));
  if (d.last_error) v.append(el("div", {class:"warn"}, "⚠ " + d.last_error));

  const seg = ["COMPLETED","RUNNING","PENDING","FAILED","CANCELLED"];
  v.append(el("div", {class:"bar"}, seg.map(s => el("i", {style:`width:${total?100*(c[s]||0)/total:0}%;background:${COLORS[s]}`, title:s}))));
  v.append(el("div", {class:"legend"}, seg.map(s => el("span", {}, el("span",{class:"dot",style:`background:${COLORS[s]}`}), `${s.toLowerCase()} ${c[s]||0}`))));

  v.append(el("div", {class:"grid"},
    [["avance", `${(d.progress||0).toFixed(1)} %`], ["ejecuciones", total],
     ["completadas", c.COMPLETED||0], ["en cola / corriendo", active], ["fallidas", failed],
     ["resultados en disco", d.on_disk], ["tiempo de pared", fmtS(e.wall_seconds)],
     ["fin previsto", e.eta || "—"]].map(([k,val]) => el("div",{class:"card"}, el("div",{class:"k"},k), el("div",{class:"v"},val)))));

  const subs = d.submissions || [];
  v.append(el("div", {class:"two"},
    el("section", {}, el("h2", {}, "Estimación"), table(["concepto","valor"], [
      ["duración por ejecución", fmtS(e.per_run_seconds)], ["origen del dato", e.source],
      ["cómputo pendiente", fmtS(e.serial_seconds)], ["tiempo de pared", fmtS(e.wall_seconds)],
      ["tareas simultáneas", e.parallelism], ["finalización prevista", e.eta || "—"]], [1])),
    el("section", {}, el("h2", {}, `Historial de envíos (${subs.length})`),
      el("div", {class:"scroll", style:"max-height:260px"}, table(["#","job array","backend","tareas","enviado"],
        subs.map(s => [s.id, s.array_job_id, s.backend, s.n_runs, (s.submitted_at||"").replace("T"," ").slice(0,19)]), [0,3])))));

  const metrics = Object.entries(d.metrics || {});
  if (metrics.length) v.append(el("section", {}, el("h2", {}, "Métricas agregadas"),
    table(["métrica","n","media","desv.","mín.","máx."], metrics.map(([k,m]) =>
      [k, m.n, num(m.mean), num(m.std), num(m.min), num(m.max)]), [1,2,3,4,5])));

  const params = Object.keys(d.sweep || {});
  const mnames = [...new Set((d.runs||[]).flatMap(r => Object.keys(r.metrics||{})))].slice(0,4);
  const tools = el("div", {class:"tools"},
    el("input", {placeholder:"buscar clave o parámetro…", value:filter.q,
      oninput:(ev)=>{filter.q = ev.target.value; drawRuns();}}),
    el("select", {onchange:(ev)=>{filter.state = ev.target.value; drawRuns();}},
      ["", "COMPLETED","RUNNING","PENDING","FAILED","CANCELLED","sin enviar"].map(s => {
        const o = el("option", {value:s}, s || "todos los estados"); if (s===filter.state) o.selected = true; return o; })),
    el("a", {href:`api/project/${encodeURIComponent(d.slug)}/runs.csv`, download:`${d.project}-ejecuciones.csv`, style:"align-self:center"}, "descargar CSV"));
  const box = el("div", {class:"scroll"});
  v.append(el("section", {}, el("h2", {}, "Ejecuciones"), tools, box));
  function drawRuns(){
    const q = filter.q.toLowerCase();
    const rows = (d.runs||[]).filter(r => {
      const st = r.state || "sin enviar";
      if (filter.state && st !== filter.state) return false;
      return !q || (r.key + " " + JSON.stringify(r.params)).toLowerCase().includes(q);
    });
    const shown = rows.slice(0, 500);
    const t = table(["#","clave",...params,"estado","disco","duración","salida",...mnames],
      shown.map(r => [r.index, r.key, ...params.map(p => r.params[p]), badge(r.state),
        badge(r.disk === "PENDIENTE" ? null : r.disk), fmtS(r.seconds), r.exit_code ?? "—",
        ...mnames.map(m => num((r.metrics||{})[m]))]),
      [0, params.length+4, params.length+5, ...mnames.map((_,i)=>params.length+6+i)]);
    box.replaceChildren(t);
    if (rows.length > shown.length) box.append(el("p", {class:"mute"}, `… y ${rows.length-shown.length} más (filtra o descarga el CSV)`));
  }
  drawRuns();

  const ef = d.efficiency, ce = d.centre || {}, fnd = d.findings || [];
  const pct = x => x == null ? "—" : `${Math.round(100*x)} %`;
  const LV = {error:"FAILED", aviso:"RUNNING", info:"PENDING"};
  v.append(el("div", {class:"two"},
    el("section", {}, el("h2", {}, "Eficiencia de recursos"),
      !ef || (!ef.n_completed && !Object.keys(ef.causes||{}).length)
        ? el("p", {class:"mute"}, "Aún no hay tareas terminadas con contabilidad (sacct).")
        : el("div", {},
            table(["indicador","valor"], [
              ["tareas completadas", ef.n_completed], ["duración p95", fmtS(ef.p95_seconds)],
              ["tiempo usado / pedido (mediana)", pct(ef.time_usage_median)],
              ["eficiencia de CPU (mediana)", pct(ef.cpu_efficiency_median)],
              ["memoria usada / pedida (mediana)", pct(ef.mem_efficiency_median)],
              ["horas-núcleo ociosas", ef.wasted_core_hours ?? "—"]], [1]),
            (ef.suggestions||[]).length ? el("div", {style:"margin-top:8px"},
              el("b", {}, "Recomendado para el próximo envío"),
              (ef.suggestions||[]).map(x => el("div", {class:"warn"},
                `${x.option}: ${x.proposed} (ahora ${x.current}) · ${x.reason}`)))
              : el("p", {class:"mute"}, "Sin cambios recomendados para el próximo envío."),
            Object.keys(ef.causes||{}).length ? el("p", {}, "Fallos por causa: ",
              Object.entries(ef.causes).map(([k,n]) => el("span", {class:"chip"}, `${k}: ${n}`))) : null)),
    el("section", {}, el("h2", {}, `Normas del centro · ${ce.title || ""}`),
      fnd.length ? fnd.map(f => el("div", {style:"margin:4px 0"}, el("span", {class:"st "+LV[f.level]}, f.level), " ", f.message))
        : el("p", {class:"mute"}, "La especificación cumple las normas conocidas del centro."),
      ce.workdir ? el("p", {}, el("b", {}, "Dónde trabajar: "), ce.workdir) : null,
      el("ul", {style:"margin:6px 0;padding-left:18px"}, (ce.notes||[]).map(n => el("li", {}, n))),
      (ce.docs||[]).length ? el("p", {class:"mute"}, "Fuentes: ",
        (ce.docs||[]).map(u => el("a", {href:u, target:"_blank", rel:"noopener", style:"margin-right:8px"}, new URL(u).hostname))) : null)));

  const fails = d.failures || [], queue = d.queue || [];
  v.append(el("div", {class:"two"},
    el("section", {}, el("h2", {}, `Fallos recientes (${fails.length})`),
      el("div", {class:"scroll", style:"max-height:280px"}, table(["envío","tarea","clave","causa","salida"],
        fails.map(f => [f.submission_id, f.task_index, f.key, f.cause || "—", f.exit_code ?? "—"]), [0,1,4]))),
    el("section", {}, el("h2", {}, `Cola del clúster (${queue.length})`),
      el("div", {class:"scroll", style:"max-height:280px"}, table(["job","estado","transcurrido","detalle"],
        queue.slice(0,200).map(q => [q.job_id, badge(q.state), fmtS(q.seconds), q.detail || ""]), [2])))));

  v.append(el("section", {}, el("h2", {}, "Barrido"),
    el("div", {}, Object.entries(d.sweep||{}).map(([k,vals]) => el("div", {},
      el("b", {}, k + ": "), vals.map(x => el("span", {class:"chip"}, x)))))));
}

async function refresh(){
  try {
    await loadSummary();
    if (current){
      const r = await fetch(`api/project/${encodeURIComponent(current)}`);
      renderProject(await r.json());
    }
    $("#clock").textContent = new Date().toLocaleTimeString("es-ES");
  } catch (err) {
    $("#view").replaceChildren(el("div", {class:"warn"}, "No se pudo contactar con el servidor: " + err));
  }
}
refresh();
setInterval(refresh, 10000);
</script>
</body>
</html>
"""
