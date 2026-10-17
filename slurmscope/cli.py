"""Interfaz de línea de comandos de SlurmScope.

Comandos: ``init``, ``plan``, ``gen``, ``submit``, ``status``, ``menu``, ``queue``,
``results``, ``estimate``, ``report``, ``sync``, ``cancel``, ``mock``, ``clusters``,
``probe``, ``push``, ``pull`` y ``serve``.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence

from . import __version__
from .backends import BackendError, RemoteSlurm, get_backend
from .clusters import (
    PROFILES,
    get_profile,
    plan_batches,
    probe_cluster,
)
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

# Dónde se ejecuta. `slurmscope clusters` lista los perfiles incluidos (Hércules,
# MareNostrum 5, FinisTerrae III, Tirant…). Con `host`, SlurmScope corre en esta
# máquina y controla el clúster por SSH (alias de ~/.ssh/config).
cluster:
  profile: hercules
  # host: hercules-cica
  # remote_dir: slurmscope/{project}

slurm:
  job_name: ejemplo
  # partition: standard     # por defecto, la del perfil o la del clúster
  # account: mi_proyecto    # obligatorio en MareNostrum 5
  # qos: gp_resb            # obligatorio en MareNostrum 5
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
        getattr(args, "backend", None) or "auto",
        mock_state=state_dir(spec) / "mock_cluster.json",
        parallel=getattr(args, "mock_parallel", None) or 4,
        failure_rate=getattr(args, "fail_rate", None) or 0.0,
        cluster=spec.cluster,
        project=spec.project,
        workdir=spec.source.parent,
    )


def submit_sweep(spec: SweepSpec, backend, ledger: Ledger, runs: Sequence,
                 artifacts: dict, log=print, indices: Optional[Sequence[int]] = None,
                 capacity: Optional[int] = None, push: bool = True) -> List[str]:
    """Envía ejecuciones del barrido respetando ``MaxArraySize`` y ``MaxSubmit``.

    ``indices`` (1-based) elige qué ejecuciones enviar; por defecto, todas. Si
    ``capacity`` está fijada (huecos libres en la cola del usuario), solo se envían
    las primeras ``capacity``: el resto queda para ``slurmscope resume``.

    Con un backend remoto, primero sube el proyecto (código, plan y especificación).
    Cada job array es un envío distinto en el libro. Si un envío falla, los
    anteriores quedan registrados y el error se propaga.
    """
    wanted = list(indices) if indices is not None else [run.index for run in runs]
    if capacity is not None:
        wanted = wanted[:max(0, capacity)]
    if not wanted:
        return []
    if push and isinstance(backend, RemoteSlurm):
        log(f"subiendo el proyecto a {backend.transport.host}:{backend.transport.remote_dir}")
        backend.transport.push(spec.source.parent, excludes=spec.cluster.push_exclude)

    by_index = {run.index: run for run in runs}
    batches = plan_batches(wanted, spec.cluster.effective_max_array_size())
    script = artifacts["script"].relative_to(spec.source.parent)
    commands = str(artifacts["commands"].relative_to(spec.source.parent))
    job_ids: List[str] = []
    for number, batch in enumerate(batches, start=1):
        env = {"SLURMSCOPE_OFFSET": str(batch.offset)} if batch.offset else None
        job_id = backend.submit(script, batch.size, job_name=spec.slurm.job_name,
                                array=batch.array_spec(spec.slurm.max_parallel), env=env)
        submission_id = ledger.record_submission(
            project=spec.project, backend=backend.name, n_runs=batch.size,
            runs=[by_index[i] for i in batch.indices], array_job_id=str(job_id),
            commands_file=commands, task_offset=batch.offset,
        )
        job_ids.append(str(job_id))
        label = f" · envío {number}/{len(batches)}" if len(batches) > 1 else ""
        log(f"envío #{submission_id} registrado · job array {job_id} · {batch.size} tareas "
            f"(ejecuciones {batch.describe()}){label}")
    return job_ids


def run_status(ledger: Ledger, project: str) -> Dict[str, str]:
    """Último estado conocido de cada ejecución (por clave)."""
    latest: Dict[str, str] = {}
    for record in ledger.runs(project=project):  # más reciente primero
        latest.setdefault(record.run_key, record.state)
    return latest


def queue_capacity(spec: SweepSpec, ledger: Ledger) -> Optional[int]:
    """Huecos libres en la cola según ``MaxSubmit`` (None si no hay límite conocido)."""
    from .rules import load_probe, submit_capacity

    cap = submit_capacity(spec, load_probe(spec))
    if cap is None:
        return None
    active = sum(1 for state in run_status(ledger, spec.project).values()
                 if state in ("PENDING", "RUNNING"))
    return max(0, cap - active)


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
    script = render_slurm_script(spec, runs, "plan/commands.txt")
    script_file.write_text(script, encoding="utf-8")
    script_file.chmod(0o755)

    manifest = plan_dir / "manifest.json"

    manifest.write_text(
        json.dumps(
            {
                "project": spec.project,
                "n_runs": len(runs),
                "sweep": {k: list(v) for k, v in spec.sweep.items()},
                "cluster": {
                    "profile": spec.cluster.profile,
                    "host": spec.cluster.host,
                    "max_array_size": spec.cluster.effective_max_array_size(),
                },
                "slurm": {
                    "partition": spec.effective_partition,
                    "account": spec.slurm.account,
                    "qos": spec.slurm.qos,
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


def _findings(spec: SweepSpec, n_runs: int):
    from .rules import check_spec, load_probe

    return check_spec(spec, n_runs, load_probe(spec))


def cmd_gen(args) -> int:
    from .rules import format_findings

    spec = _load(args)
    runs = expand_runs(spec)
    artifacts = _generate(spec, runs, force=args.force)
    print(f"{len(runs)} ejecuciones planificadas")
    print(f"  comandos : {artifacts['commands']}")
    print(f"  script   : {artifacts['script']}")
    print(f"  manifiesto: {artifacts['manifest']}")
    batches = plan_batches([r.index for r in runs], spec.cluster.effective_max_array_size())
    if len(batches) > 1:
        print(f"  troceo    : {len(batches)} job arrays (MaxArraySize="
              f"{spec.cluster.effective_max_array_size()})")
    findings = _findings(spec, len(runs))
    if findings:
        print("\nnormas del centro:")
        print(format_findings(findings))
    if args.backend == "mock":
        print("\nlanza el ciclo simulado con:  slurmscope submit --backend mock")
    else:
        print("\nenvía y registra en el libro con:  slurmscope submit")
    return 0


def cmd_check(args) -> int:
    """Contrasta la especificación con las normas documentadas del centro."""
    from .rules import effective_limits, format_findings, has_errors, load_probe

    spec = _load(args)
    runs = expand_runs(spec)
    probe = load_probe(spec)
    findings = _findings(spec, len(runs))
    if args.json:
        print(json.dumps([f.as_dict() for f in findings], indent=2, ensure_ascii=False))
        return 1 if has_errors(findings) else 0
    profile = spec.profile
    name, limits = effective_limits(spec, probe)
    print(f"centro         : {profile.title} · {profile.centre}"
          + (" · nodo RES" if profile.res_node else ""))
    print(f"partición/QoS  : {spec.effective_partition or 'por defecto'} / "
          f"{spec.effective_qos or ('automática' if profile.qos_is_automatic else 'por defecto')}")
    print(f"límites        : {name or '—'} · máx. {limits.max_wall or '?'} · "
          f"{limits.max_jobs or '?'} a la vez · {limits.max_submit or '?'} en cola")
    if spec.cluster.max_array_size:
        source = "especificación"
    elif profile.max_array_size:
        source = "perfil"
    else:
        source = "valor por defecto de SLURM"
    probed = (probe or {}).get("max_array_size")
    print(f"MaxArraySize   : {spec.cluster.effective_max_array_size()} ({source})"
          + (f" · el clúster dice {probed}" if probed else ""))
    print(f"ejecuciones    : {len(runs)}")
    if profile.workdir_hint:
        print(f"dónde trabajar : {profile.workdir_hint}")
    print()
    print(format_findings(findings))
    return 1 if has_errors(findings) else 0


def _confirm(prompt: str) -> bool:
    answer = input(prompt)
    return answer.strip().lower() in {"s", "y", "si", "sí"}


def cmd_submit(args) -> int:
    from .rules import format_findings, has_errors

    spec = _load(args)
    runs = expand_runs(spec)
    # el envío siempre regenera sus artefactos para que el plan y el envío coincidan
    artifacts = _generate(spec, runs, force=True)
    backend = _backend_for(args, spec)

    if backend.name != "mock":
        findings = _findings(spec, len(runs))
        if has_errors(findings):
            print(format_findings([f for f in findings if f.level == "error"]), file=sys.stderr)
            print("\nno se envía nada: corrige los errores (detalle: slurmscope check)",
                  file=sys.stderr)
            return 2
        warnings = [f for f in findings if f.level != "info"]
        if warnings:
            print(format_findings(warnings))

    ledger = open_ledger(spec)
    capacity = queue_capacity(spec, ledger)  # también en el simulador: imita las normas
    indices = [run.index for run in runs]
    to_send = indices if capacity is None else indices[:capacity]
    batches = plan_batches(to_send, spec.cluster.effective_max_array_size()) if to_send else []
    where = backend.name
    if args.dry_run:
        print(f"dry-run: no se envía nada ({where})")
        for batch in batches:
            env = f" --export=ALL,SLURMSCOPE_OFFSET={batch.offset}" if batch.offset else ""
            print(f"  sbatch --parsable --array={batch.array_spec(spec.slurm.max_parallel)}"
                  f"{env} plan/submit_array.slurm   # ejecuciones {batch.describe()}")
        if len(to_send) < len(indices):
            print(f"  ({len(indices) - len(to_send)} ejecuciones quedarían para 'slurmscope resume')")
        return 0
    if not to_send:
        print("la cola está llena según MaxSubmit: espera a que avance y usa 'slurmscope resume'")
        return 1

    ok, reason = backend.available()
    if not ok:
        print(f"backend no disponible: {reason}", file=sys.stderr)
        return 1

    if backend.name != "mock" and not args.yes:
        if not _confirm(f"¿Enviar {len(to_send)} ejecuciones en {len(batches)} job array(s) "
                        f"a {where}? [s/N] "):
            print("cancelado")
            return 1

    try:
        submit_sweep(spec, backend, ledger, runs, artifacts, indices=to_send)
    except BackendError as exc:
        print(f"error al enviar: {exc}", file=sys.stderr)
        return 1
    if len(to_send) < len(indices):
        print(f"quedan {len(indices) - len(to_send)} ejecuciones por enviar (límite de cola): "
              "slurmscope resume")
    print("sigue el avance con:  slurmscope status   ·   slurmscope menu   ·   slurmscope serve")
    return 0


def cmd_resume(args) -> int:
    """Envía lo que falta: ejecuciones nunca enviadas y, con --failed, las fallidas."""
    spec = _load(args)
    runs = expand_runs(spec)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)
    if not args.no_sync:
        sync_states(ledger, backend, project=spec.project)
    status = run_status(ledger, spec.project)
    retry = {"FAILED", "CANCELLED"} if args.failed else set()
    indices = [run.index for run in runs
               if status.get(run.key) is None or status.get(run.key) in retry]
    if not indices:
        print("no hay nada pendiente de enviar"
              + ("" if args.failed else " (usa --failed para relanzar las fallidas)"))
        return 0
    capacity = queue_capacity(spec, ledger)  # también en el simulador: imita las normas
    to_send = indices if capacity is None else indices[:capacity]
    if not to_send:
        print(f"{len(indices)} ejecuciones pendientes, pero la cola está llena (MaxSubmit): "
              "vuelve a intentarlo cuando avance")
        return 1
    artifacts = _generate(spec, runs, force=True)
    if args.dry_run:
        for batch in plan_batches(to_send, spec.cluster.effective_max_array_size()):
            print(f"  --array={batch.array_spec(spec.slurm.max_parallel)} offset={batch.offset}"
                  f"   # ejecuciones {batch.describe()}")
        return 0
    if backend.name != "mock" and not args.yes:
        if not _confirm(f"¿Enviar {len(to_send)} de {len(indices)} ejecuciones pendientes a "
                        f"{backend.name}? [s/N] "):
            print("cancelado")
            return 1
    try:
        submit_sweep(spec, backend, ledger, runs, artifacts, indices=to_send)
    except BackendError as exc:
        print(f"error al enviar: {exc}", file=sys.stderr)
        return 1
    rest = len(indices) - len(to_send)
    print(f"enviadas {len(to_send)} ejecuciones" + (f" · quedan {rest}" if rest else ""))
    return 0


def cmd_acknowledge(args) -> int:
    """Texto de agradecimiento que exige el centro para las publicaciones."""
    from .clusters_data import RES_ACKNOWLEDGEMENT, RES_ACKNOWLEDGEMENT_NOTE

    if args.name:
        profile = get_profile(args.name)
    else:
        profile = _load(args).profile
    print(f"{profile.title} · {profile.centre}\n")
    if profile.acknowledgement:
        print(profile.acknowledgement)
    else:
        print("El centro no publica un texto de agradecimiento propio.")
    if profile.res_node:
        if profile.acknowledgement != RES_ACKNOWLEDGEMENT:
            print("\nCon horas de la RES, además:\n" + RES_ACKNOWLEDGEMENT)
        print("\n" + RES_ACKNOWLEDGEMENT_NOTE)
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
            job_ids = submit_sweep(spec, backend, ledger, runs, artifacts, log=lambda _m: None)
        except BackendError as exc:
            return f"error al enviar: {exc}"
        return "enviado: job array " + ", ".join(job_ids)

    def do_cancel() -> Optional[str]:
        job_ids = _active_job_ids(ledger, spec.project)
        if not job_ids:
            return "no hay job arrays activos que cancelar"
        try:
            for job_id in job_ids:
                backend.cancel(job_id)
        except BackendError as exc:
            return f"error al cancelar: {exc}"
        sync_states(ledger, backend, project=spec.project)
        return "cancelado " + ", ".join(job_ids)

    actions.append(MenuAction("e", "Enviar barrido", do_submit))
    actions.append(MenuAction("c", "Cancelar envíos activos", do_cancel))
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
    json_path = write_json(dash, Path(args.json) if args.json else out_dir / f"{spec.project}-estado.json", ledger)
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


def _active_job_ids(ledger: Ledger, project: str) -> List[str]:
    """Job arrays del proyecto con alguna tarea todavía pendiente o en ejecución."""
    by_submission = {s.id: s for s in ledger.submissions(project=project)}
    active = []
    for record in ledger.runs(project=project):
        if record.state in ("PENDING", "RUNNING"):
            submission = by_submission.get(record.submission_id)
            if submission and submission.array_job_id and submission.array_job_id not in active:
                active.append(str(submission.array_job_id))
    return active


def cmd_cancel(args) -> int:
    spec = _load(args)
    ledger = open_ledger(spec)
    backend = _backend_for(args, spec)
    if args.array_job_id:
        job_ids = [args.array_job_id]
    else:
        job_ids = _active_job_ids(ledger, spec.project)
        if not job_ids:
            print("no hay job arrays activos que cancelar", file=sys.stderr)
            return 1
    for job_id in job_ids:
        try:
            backend.cancel(job_id)
        except BackendError as exc:
            print(f"error al cancelar {job_id}: {exc}", file=sys.stderr)
            return 1
    # el libro debe reflejar la cancelación, no quedarse en PENDING
    result = sync_states(ledger, backend, project=spec.project)
    print(f"cancelado {', '.join(job_ids)} · {result['updated']} tareas actualizadas en el libro")
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


def cmd_clusters(args) -> int:
    """Lista los perfiles de clúster incluidos."""
    if args.json:
        print(json.dumps({k: p.as_dict() for k, p in PROFILES.items()}, indent=2,
                         ensure_ascii=False))
        return 0
    if not args.name:
        rows = []
        for profile in PROFILES.values():
            rules = [label for flag, label in ((profile.requires_account, "account"),
                                               (profile.requires_qos, "qos"),
                                               (profile.requires_partition, "partición"),
                                               (profile.qos_is_automatic, "qos auto"))
                     if flag]
            rows.append([profile.name, profile.title, profile.centre,
                         "sí" if profile.res_node else "", profile.login or "—",
                         profile.max_array_size or "?", "+".join(rules) or "—"])
        print(text_table(["perfil", "máquina", "centro", "RES", "login", "max array",
                          "reglas"], rows, ["l", "l", "l", "l", "l", "r", "l"]))
        print("\n'?' = no documentado: `slurmscope probe` lo lee del clúster.")
        print("detalle de un perfil: slurmscope clusters --name hercules")
        return 0
    profile = get_profile(args.name)
    print(f"{profile.title} · {profile.centre}" + (" · nodo RES" if profile.res_node else ""))
    if profile.logins:
        print(f"login        : {', '.join(profile.logins)}")
    if profile.access:
        print(f"acceso       : {profile.access}")
    print(f"partición    : {profile.partition or 'la del clúster'}"
          + (f" · RES: {profile.res_partition}" if profile.res_partition else ""))
    if profile.res_qos and not profile.qos_is_automatic:
        print(f"QoS RES      : {profile.res_qos}")
    if profile.workdir_hint:
        print(f"dónde trabajar: {profile.workdir_hint}")
    if profile.internet_from_cluster is False:
        print("internet     : sin conexiones salientes desde el clúster")
    table = {**profile.partition_limits, **profile.qos_limits}
    if table:
        print()
        print(text_table(["QoS/partición", "tiempo máx.", "a la vez", "en cola", "nota"],
                         [[k, v.max_wall or "?", v.max_jobs or "?", v.max_submit or "?", v.note]
                          for k, v in table.items()], ["l", "r", "r", "r", "l"]))
    if profile.directives or profile.preamble:
        print()
        for line in profile.directives + profile.preamble:
            print(f"se añade al script: {line}")
    print()
    for note in profile.notes:
        print(f"· {note}")
    if profile.setup_hint:
        print(f"· software: {profile.setup_hint}")
    for doc in profile.docs:
        print(f"· documentación: {doc}")
    if profile.verified:
        print(f"· comprobado: {profile.verified}")
    return 0


def cmd_probe(args) -> int:
    """Pregunta al clúster por sus límites y los guarda en .slurmscope/probe.json."""
    spec = _load(args)
    backend = _backend_for(args, spec)
    if backend.name == "mock":
        print("el simulador no tiene nada que sondear", file=sys.stderr)
        return 1
    if isinstance(backend, RemoteSlurm):
        runner = lambda argv: backend.transport.run(argv, in_project=False)  # noqa: E731
    else:
        runner = backend._run
    result = probe_cluster(runner)
    (state_dir(spec) / "probe.json").write_text(
        json.dumps(result.as_dict(), indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if args.json:
        print(json.dumps(result.as_dict(), indent=2, ensure_ascii=False))
        return 0
    print(f"clúster        : {backend.name}")
    print(f"SLURM          : {result.slurm_version or '?'}")
    print(f"MaxArraySize   : {result.max_array_size or '?'}"
          f"   (en uso: {spec.cluster.effective_max_array_size()})")
    print(f"MaxJobCount    : {result.max_job_count or '?'}")
    print(f"Python (login) : {result.python or '?'}")
    if result.partitions:
        print()
        print(text_table(["partición", "defecto", "estado", "límite", "libres", "nodos"],
                         [[p["name"], p["default"], p["state"], p["time_limit"],
                           p["idle_nodes"], p["total_nodes"]] for p in result.partitions],
                         ["l", "l", "l", "r", "r", "r"]))
    if result.associations:
        print()
        print(text_table(["cuenta", "partición", "qos"],
                         [[a["account"], a["partition"] or "(todas)", a["qos"]]
                          for a in result.associations]))
    if (result.max_array_size and
            result.max_array_size != spec.cluster.effective_max_array_size()):
        print(f"\naviso: el clúster dice MaxArraySize={result.max_array_size}; "
              f"añade 'cluster.max_array_size: {result.max_array_size}' a la especificación")
    for warning in result.warnings:
        print(f"aviso: {warning}", file=sys.stderr)
    return 0


def _remote(args, spec: SweepSpec) -> RemoteSlurm:
    backend = _backend_for(args, spec)
    if not isinstance(backend, RemoteSlurm):
        raise ConfigError("esta orden necesita un clúster remoto: define 'cluster.host'")
    return backend


def cmd_push(args) -> int:
    spec = _load(args)
    backend = _remote(args, spec)
    out = backend.transport.push(spec.source.parent, excludes=spec.cluster.push_exclude)
    changed = [line for line in out.splitlines() if line.strip()]
    print(f"subido a {backend.transport.host}:{backend.transport.remote_dir} "
          f"· {len(changed)} cambios")
    return 0


def cmd_pull(args) -> int:
    spec = _load(args)
    backend = _remote(args, spec)
    what = ["results"] + (["logs"] if args.logs else [])
    out = backend.transport.pull(spec.source.parent, what=what)
    changed = [line for line in out.splitlines() if line.strip()]
    print(f"descargado {', '.join(what)} de {backend.transport.host} · {len(changed)} cambios")
    return 0


def cmd_serve(args) -> int:
    from .web import serve

    directories = args.project or [args.directory or "."]
    return serve(
        [Path(d) for d in directories],
        host=args.host,
        port=args.port,
        sync_interval=args.sync_interval,
        backend=getattr(args, "backend", None) or "auto",
        demo_tick=args.demo_tick,
    )


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
        "--backend", default=argparse.SUPPRESS, choices=["auto", "slurm", "ssh", "mock"]
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
    parser.set_defaults(directory=None, spec=None, backend="auto", mock_parallel=4, fail_rate=0.0)

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

    p = add("check", "contrasta la especificación con las normas del centro")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_check)

    p = add("resume", "envía lo pendiente (oleadas por MaxSubmit) y, con --failed, relanza")
    p.add_argument("--failed", action="store_true", help="relanza también fallidas y canceladas")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-sync", action="store_true")
    p.add_argument("--yes", "-y", action="store_true")
    p.set_defaults(func=cmd_resume)

    p = add("acknowledge", "texto de agradecimiento que exige el centro en publicaciones")
    p.add_argument("--name", help="perfil (por defecto, el de la especificación)")
    p.set_defaults(func=cmd_acknowledge)

    p = add("clusters", "perfiles de clúster incluidos (RES, Hércules…)")
    p.add_argument("--name", help="detalle de un perfil")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_clusters)

    p = add("probe", "lee del clúster sus límites, particiones y cuentas")
    p.add_argument("--json", action="store_true")
    p.set_defaults(func=cmd_probe)

    p = add("push", "sube el proyecto al clúster remoto (rsync)")
    p.set_defaults(func=cmd_push)

    p = add("pull", "descarga los resultados del clúster remoto (rsync)")
    p.add_argument("--logs", action="store_true", help="descarga también los logs")
    p.set_defaults(func=cmd_pull)

    p = add("serve", "panel web de uno o varios barridos")
    p.add_argument("--project", action="append", metavar="DIR",
                   help="directorio de un barrido (repetible)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8120)
    p.add_argument("--sync-interval", type=float, default=60.0,
                   help="segundos entre sincronizaciones con el clúster (0 = nunca)")
    p.add_argument("--demo-tick", type=float, default=0.0, metavar="S",
                   help="solo simulador: avanza S segundos simulados en cada sincronización "
                        "y relanza el barrido cuando acaba (demostraciones)")
    p.set_defaults(func=cmd_serve)

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
