"""Interfaz de línea de comandos de SlurmScope.

Comandos: ``init``, ``plan``, ``gen``, ``submit``, ``status``, ``menu``, ``queue``,
``results``, ``estimate``, ``report``, ``sync``, ``cancel`` y ``mock``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional, Sequence

from . import __version__
from .backends import BackendError, get_backend
from .config import ConfigError, SweepSpec, load_spec
from .estimate import estimate_remaining, format_seconds, limit_seconds
from .ledger import Ledger
from .matrix import expand_runs, plan_summary, render_slurm_script, write_command_file
from .report import (
    build_dashboard,
    render_overview,
    render_results,
    render_runs,
    text_table,
    write_json,
    write_markdown,
)
from .sync import sync_states
from .tui import MenuAction, run_menu

TEMPLATE_SPEC = """\
# Especificación de barrido para SlurmScope
project: ejemplo-sweep

# Plantilla del directorio/nombre de cada ejecución (y clave única)
run_id: "{dataset}/q{qubits}_d{depth}_s{seed}"

# Plantilla del comando. Puede usar cualquier parámetro del barrido,
# además de {index}, {key}, {run_dir} y {plan_out}.
command: >-
  python runner.py --dataset {dataset} --qubits {qubits} --depth {depth}
  --seed {seed} --out {run_dir}

results_root: results

# Producto cartesiano de estos parámetros
sweep:
  dataset: [mnist, fashion]
  qubits: [4, 6]
  depth: [1, 3]
  seed: [1, 2, 3]

slurm:
  job_name: ejemplo
  partition: standard
  time: "08:00:00"
  cpus_per_task: 4
  mem: 16G
  max_parallel: 20          # tope de tareas simultáneas (%N del job array)
  log_dir: logs
  conda_base: /lustre/software/easybuild/common/software/Miniconda3/4.9.2
  conda_env: qtl
  setup: ""                 # p. ej. "module load Python/3.11.3-GCCcore-12.3.0"
  extra_sbatch: []          # p. ej. ["#SBATCH --gres=gpu:1"]
"""

GITIGNORE_BLOCK = """.slurmscope/
logs/
results/
plan/
"""


def state_dir(spec: SweepSpec) -> Path:
    """Directorio de estado del proyecto (``.slurmscope/``)."""
    path = spec.source.parent / ".slurmscope"
    path.mkdir(parents=True, exist_ok=True)
    return path


def open_ledger(spec: SweepSpec) -> Ledger:
    return Ledger(state_dir(spec) / "ledger.db")


def _backend_for(args, spec: SweepSpec):
    return get_backend(
        getattr(args, "backend", "slurm"),
        mock_state=state_dir(spec) / "mock_cluster.json",
        parallel=getattr(args, "mock_parallel", 4),
        failure_rate=getattr(args, "fail_rate", 0.0),
    )


def _load(args) -> SweepSpec:
    if getattr(args, "directory", None):
        os.chdir(args.directory)
    return load_spec(getattr(args, "spec", None))


def _generate(spec: SweepSpec, runs: Sequence, force: bool = False) -> dict:
    """Escribe el fichero de comandos y el script de job array.

    Crea además los directorios de logs y de resultados en la máquina desde la que
    se envía: SLURM crea los ficheros de salida pero no sus directorios, así que si
    ``logs/`` no existe el job falla al arrancar.
    """
    plan_dir = spec.source.parent / "plan"
    plan_dir.mkdir(parents=True, exist_ok=True)
    commands_file = plan_dir / "commands.txt"
    script_file = plan_dir / "submit_array.slurm"

    if commands_file.exists() and not force:
        raise ConfigError(
            f"{commands_file} ya existe (usa --force para sobrescribirlo)"
        )

    log_dir = spec.resolve(spec.slurm.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    spec.results_path().mkdir(parents=True, exist_ok=True)

    write_command_file(spec, runs, commands_file)
    script = render_slurm_script(spec, runs, str(commands_file))
    script_file.write_text(script, encoding="utf-8")
    script_file.chmod(0o755)

    manifest = plan_dir / "manifest.json"

    manifest.write_text(
        json.dumps(
            {
                "project": spec.project,
                "n_runs": len(runs),
                "sweep": {k: list(v) for k, v in spec.sweep.items()},
                "slurm": {
                    "partition": spec.slurm.partition,
                    "time": spec.slurm.time,
                    "cpus_per_task": spec.slurm.cpus_per_task,
                    "mem": spec.slurm.mem,
                    "max_parallel": spec.slurm.max_parallel,
                },
                "runs": [run.as_dict() for run in runs],
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return {"commands": commands_file, "script": script_file, "manifest": manifest}


# ───────────────────────────── comandos ──────────────────────────────────────

def cmd_init(args) -> int:
    target = Path(args.directory or Path.cwd())
    target.mkdir(parents=True, exist_ok=True)
    spec_path = target / "sweep.yaml"
    if spec_path.exists() and not args.force:
        print(f"ya existe {spec_path} (usa --force para sobrescribirlo)")
        return 1
    spec_path.write_text(TEMPLATE_SPEC, encoding="utf-8")

    gitignore = target / ".gitignore"
    existing = gitignore.read_text(encoding="utf-8") if gitignore.exists() else ""
    if ".slurmscope/" not in existing:
        gitignore.write_text(existing + GITIGNORE_BLOCK, encoding="utf-8")

    print(f"creado {spec_path}")
    print("siguiente paso: edita 'command' y 'sweep' y ejecuta `slurmscope plan`")
    return 0


def cmd_plan(args) -> int:
    spec = _load(args)
    runs = expand_runs(spec)
    print(plan_summary(spec, runs, limit=args.limit))
    return 0


def cmd_gen(args) -> int:
    spec = _load(args)
    runs = expand_runs(spec)
    artifacts = _generate(spec, runs, force=args.force)
    print(f"{len(runs)} ejecuciones planificadas")
    print(f"  comandos : {artifacts['commands']}")
    print(f"  script   : {artifacts['script']}")
    print(f"  manifiesto: {artifacts['manifest']}")
    if args.backend == "mock":
        print("\nlanza el ciclo simulado con:  slurmscope submit --backend mock")
    else:
        print("\nenvía con:  sbatch " + str(artifacts["script"]))
        print("o con registro en el libro:  slurmscope submit")
    return 0


def cmd_submit(args) -> int:
    spec = _load(args)
    runs = expand_runs(spec)
    # el envío siempre regenera sus artefactos para que el plan y el envío coincidan
    artifacts = _generate(spec, runs, force=True)
    backend = _backend_for(args, spec)

    ok, reason = backend.available()
    if not ok:
        print(f"backend no disponible: {reason}", file=sys.stderr)
        return 1

    command = ["sbatch", str(artifacts["script"])]
    if args.dry_run:
        print("dry-run: no se envía nada")
        print("  " + " ".join(command))
        print(f"  ({len(runs)} tareas)")
        return 0

    if args.backend != "mock" and not args.yes:
        answer = input(f"¿Enviar {len(runs)} ejecuciones con {spec.slurm.partition}? [s/N] ")
        if answer.strip().lower() not in {"s", "y", "si", "sí"}:
            print("cancelado")
            return 1

    try:
        job_id = backend.submit(artifacts["script"], len(runs), job_name=spec.slurm.job_name)
    except BackendError as exc:
        print(f"error al enviar: {exc}", file=sys.stderr)
        return 1

    ledger = open_ledger(spec)
    submission_id = ledger.record_submission(
        project=spec.project,
        backend=backend.name,
        n_runs=len(runs),
        runs=runs,
        array_job_id=str(job_id),
        commands_file=str(artifacts["commands"]),
    )
    print(f"envío #{submission_id} registrado · job array {job_id} · {len(runs)} tareas")
    print("sigue el avance con:  slurmscope status   o   slurmscope menu")
    return 0


def cmd_status(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = None if args.no_sync else _backend_for(args, spec)
    dash = build_dashboard(spec, ledger, backend=backend, sync=not args.no_sync)
    print(render_overview(dash))
    if args.detail:
        print()
        print(render_runs(dash, limit=args.detail))
    return 0


def cmd_menu(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)

    def refresh():
        return build_dashboard(spec, ledger, backend=backend, sync=True)

    actions: List[MenuAction] = []

    def do_submit() -> Optional[str]:
        runs = expand_runs(spec)
        artifacts = _generate(spec, runs, force=True)
        try:
            job_id = backend.submit(
                artifacts["script"], len(runs), job_name=spec.slurm.job_name
            )
        except BackendError as exc:
            return f"error al enviar: {exc}"
        ledger.record_submission(
            project=spec.project, backend=backend.name, n_runs=len(runs), runs=runs,
            array_job_id=str(job_id), commands_file=str(artifacts["commands"]),
        )
        return f"enviado: job array {job_id}"

    def do_cancel() -> Optional[str]:
        submissions = ledger.submissions(project=spec.project)
        active = [s for s in submissions if s.array_job_id]
        if not active:
            return "no hay envíos que cancelar"
        try:
            backend.cancel(str(active[0].array_job_id))
        except BackendError as exc:
            return f"error al cancelar: {exc}"
        return f"cancelado el job array {active[0].array_job_id}"

    actions.append(MenuAction("e", "Enviar barrido", do_submit))
    actions.append(MenuAction("c", "Cancelar último envío", do_cancel))
    return run_menu(refresh, actions, force_plain=args.plain)


def cmd_queue(args) -> int:
    spec = _load(args)
    backend = _backend_for(args, spec)
    try:
        entries = backend.queue()
    except BackendError as exc:
        print(f"no se pudo consultar la cola: {exc}", file=sys.stderr)
        return 1
    if not entries:
        print("cola vacía")
        return 0
    print(
        text_table(
            ["job", "nombre", "estado", "transcurrido", "detalle"],
            [[e.job_id, e.name, e.state, format_seconds(e.seconds), e.detail] for e in entries],
            ["l", "l", "l", "r", "l"],
        )
    )
    return 0


def cmd_results(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    dash = build_dashboard(spec, ledger, backend=None, sync=False)
    print(render_results(dash, limit=args.limit))
    return 0


def cmd_estimate(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    runs = expand_runs(spec)
    estimate = estimate_remaining(
        ledger,
        n_total=len(runs),
        parallelism=spec.slurm.max_parallel or len(runs) or 1,
        fallback_seconds=limit_seconds(spec),
        project=spec.project,
        use_p90=args.p90,
    )
    from .report import render_estimate_table

    print(render_estimate_table(estimate))
    return 0


def cmd_report(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = None if args.no_sync else _backend_for(args, spec)
    dash = build_dashboard(spec, ledger, backend=backend, sync=not args.no_sync)

    out_dir = Path(args.out) if args.out else spec.source.parent / "reports"
    md_path = write_markdown(dash, Path(args.md) if args.md else out_dir / f"{spec.project}-informe.md")
    json_path = write_json(dash, Path(args.json) if args.json else out_dir / f"{spec.project}-estado.json")
    csv_path = ledger.export_csv(
        Path(args.csv) if args.csv else out_dir / f"{spec.project}-ejecuciones.csv",
        project=spec.project,
    )
    print(f"informe Markdown: {md_path}")
    print(f"estado JSON     : {json_path}")
    print(f"ejecuciones CSV : {csv_path}")
    return 0


def cmd_sync(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)
    result = sync_states(ledger, backend, project=spec.project)
    print(
        f"actualizadas {result['updated']} tareas "
        f"(en cola: {result['queued']}, contabilidad: {result['accounted']})"
    )
    for warning in result.get("warnings", []):
        print(f"aviso: {warning}", file=sys.stderr)
    return 0


def cmd_cancel(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)
    job_id = args.array_job_id
    if not job_id:
        submissions = [s for s in ledger.submissions(project=spec.project) if s.array_job_id]
        if not submissions:
            print("no hay envíos registrados", file=sys.stderr)
            return 1
        job_id = str(submissions[0].array_job_id)
    try:
        backend.cancel(job_id)
    except BackendError as exc:
        print(f"error al cancelar: {exc}", file=sys.stderr)
        return 1
    # el libro debe reflejar la cancelación, no quedarse en PENDING
    result = sync_states(ledger, backend, project=spec.project)
    print(f"cancelado {job_id} · {result['updated']} tareas actualizadas en el libro")
    return 0


def cmd_mock(args) -> int:
    """Avanza el clúster simulado (para pruebas y demostraciones)."""
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)
    if not hasattr(backend, "advance"):  # pragma: no cover - defensivo
        print("el backend activo no es el simulador", file=sys.stderr)
        return 1
    consumed = backend.advance(args.advance)
    result = sync_states(ledger, backend, project=spec.project)
    print(
        f"simulación avanzada {format_seconds(consumed)} · "
        f"{result['updated']} tareas actualizadas · "
        f"pendientes en el clúster simulado: {backend.pending_count()}"
    )
    return 0


def cmd_version(args) -> int:
    print(f"slurmscope {__version__}")
    return 0


# ───────────────────────────── parser ────────────────────────────────────────

def _common_flags() -> argparse.ArgumentParser:
    """Opciones globales, replicadas en cada subcomando con SUPPRESS.

    Así ``slurmscope submit --backend mock`` y ``slurmscope --backend mock submit``
    se comportan igual: las copias del subcomando no pisan el valor ya leído si el
    usuario no las repite.
    """
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("-C", "--directory", default=argparse.SUPPRESS)
    common.add_argument("-s", "--spec", default=argparse.SUPPRESS)
    common.add_argument(
        "--backend", default=argparse.SUPPRESS, choices=["slurm", "mock"]
    )
    common.add_argument("--mock-parallel", type=int, default=argparse.SUPPRESS)
    common.add_argument("--fail-rate", type=float, default=argparse.SUPPRESS)
    return common


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="slurmscope",
        description="Orquestador de barridos de experimentos en clústeres SLURM",
        parents=[_common_flags()],
    )
    parser.add_argument("--version", action="version", version=f"slurmscope {__version__}")
    parser.set_defaults(directory=None, spec=None, backend="slurm", mock_parallel=4, fail_rate=0.0)

    for action in parser._actions:  # los globales también aceptan valor por defecto real
        if action.dest in {"directory", "spec", "backend", "mock_parallel", "fail_rate"}:
            if action.default is argparse.SUPPRESS:
                action.default = None

    sub = parser.add_subparsers(dest="command", required=True)
    common = _common_flags()

    def add(name: str, help_text: str) -> argparse.ArgumentParser:
        return sub.add_parser(name, help=help_text, parents=[common])

    p = add("init", "crea una especificación de ejemplo")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_init)

    p = add("plan", "muestra el plan de ejecuciones")
    p.add_argument("--limit", type=int, default=3)
    p.set_defaults(func=cmd_plan)

    p = add("gen", "genera el fichero de comandos y el job array")
    p.add_argument("--force", action="store_true")
    p.set_defaults(func=cmd_gen)

    p = add("submit", "envía el barrido y lo registra")
    p.add_argument("--force", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--yes", "-y", action="store_true")
    p.set_defaults(func=cmd_submit)

    p = add("status", "resumen del estado del barrido")
    p.add_argument("--detail", type=int, default=0, metavar="N",
                   help="además, lista N ejecuciones")
    p.add_argument("--no-sync", action="store_true")
    p.set_defaults(func=cmd_status)

    p = add("menu", "panel interactivo")
    p.add_argument("--plain", action="store_true", help="fuerza el menú de texto")
    p.set_defaults(func=cmd_menu)

    p = add("queue", "estado de la cola del clúster")
    p.set_defaults(func=cmd_queue)

    p = add("results", "resultados encontrados en disco")
    p.add_argument("--limit", type=int, default=30)
    p.set_defaults(func=cmd_results)

    p = add("estimate", "estimación de tiempos restantes")
    p.add_argument("--p90", action="store_true", help="usa el percentil 90 en vez de la mediana")
    p.set_defaults(func=cmd_estimate)

    p = add("report", "exporta el informe (Markdown/JSON/CSV)")
    p.add_argument("--out")
    p.add_argument("--md")
    p.add_argument("--json")
    p.add_argument("--csv")
    p.add_argument("--no-sync", action="store_true")
    p.set_defaults(func=cmd_report)

    p = add("sync", "vuelca el estado del clúster en el libro")
    p.set_defaults(func=cmd_sync)

    p = add("cancel", "cancela un job array")
    p.add_argument("--array-job-id")
    p.set_defaults(func=cmd_cancel)

    p = add("mock", "avanza el clúster simulado (segundos)")
    p.add_argument("--advance", type=float, required=True)
    p.set_defaults(func=cmd_mock)

    p = add("version", "muestra la versión")
    p.set_defaults(func=cmd_version)

    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"error de configuración: {exc}", file=sys.stderr)
        return 2
    except BackendError as exc:
        print(f"error de clúster: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\ninterrumpido")
        return 130


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
