"""Backend remoto por SSH, envío troceado y reglas de cada centro.

No se abre ninguna conexión: el transporte SSH recibe un ejecutor falso que
registra las órdenes y responde como lo haría el clúster.
"""

from __future__ import annotations

import io
import os
import shlex
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.backends import (  # noqa: E402
    BackendError,
    MockSlurm,
    RealSlurm,
    RemoteSlurm,
    SSHTransport,
    get_backend,
)
from slurmscope.cli import _generate, main, submit_sweep  # noqa: E402
from slurmscope.clusters import ClusterConfig  # noqa: E402
from slurmscope.config import load_spec  # noqa: E402
from slurmscope.ledger import Ledger  # noqa: E402
from slurmscope.matrix import expand_runs, render_slurm_script  # noqa: E402
from slurmscope.sync import sync_states  # noqa: E402


class FakeCluster:
    """Hace de ``ssh``/``rsync``: guarda cada orden y contesta según el comando."""

    def __init__(self):
        self.calls = []
        self.next_job = 5316000

    def __call__(self, argv, timeout=None):
        self.calls.append(list(argv))
        if argv[0] == "rsync":
            return ">f+++++++++ sweep.yaml\n"
        remote = argv[-1]
        if "whoami" in remote:
            return "usuario\n4\n"
        if "sbatch" in remote:
            self.next_job += 1
            return f"{self.next_job};hercules\n"
        if "squeue" in remote or "sacct" in remote or "mkdir" in remote:
            return ""
        return ""

    def remote_commands(self):
        return [c[-1] for c in self.calls if c[0] == "ssh"]


class TestTransporteSSH(unittest.TestCase):
    def setUp(self):
        self.fake = FakeCluster()
        self.transport = SSHTransport("hercules-cica", remote_dir="~/slurmscope/qtl",
                                      runner=self.fake)

    def test_opciones_seguras_por_defecto(self):
        self.transport.run(["squeue"])
        argv = self.fake.calls[0]
        self.assertEqual(argv[0], "ssh")
        self.assertIn("BatchMode=yes", argv)  # nunca se queda esperando una contraseña
        self.assertEqual(argv[-2], "hercules-cica")

    def test_entra_en_el_directorio_del_proyecto(self):
        self.transport.run(["sbatch", "--parsable", "plan/submit_array.slurm"])
        self.assertEqual(
            self.fake.calls[0][-1],
            "cd slurmscope/qtl && sbatch --parsable plan/submit_array.slurm",
        )

    def test_la_tilde_se_convierte_en_ruta_relativa_al_home(self):
        self.assertEqual(self.transport.remote_dir, "slurmscope/qtl")

    def test_argumentos_entrecomillados(self):
        self.transport.run(["echo", "a b; rm -rf ~"], in_project=False)
        remote = self.fake.calls[0][-1]
        self.assertEqual(shlex.split(remote), ["echo", "a b; rm -rf ~"])

    def test_host_peligroso(self):
        with self.assertRaises(BackendError):
            SSHTransport("-oProxyCommand=evil")

    def test_push_no_borra_y_excluye_estado(self):
        self.transport.push(Path("/tmp/proyecto"), excludes=["data/"])
        mkdir = self.fake.calls[0][-1]
        self.assertIn("mkdir -p slurmscope/qtl slurmscope/qtl/logs slurmscope/qtl/results", mkdir)
        rsync = self.fake.calls[1]
        self.assertEqual(rsync[0], "rsync")
        self.assertNotIn("--delete", rsync)
        for pattern in (".slurmscope/", "results/", "logs/", ".git/", "data/"):
            self.assertIn(pattern, rsync)
        self.assertEqual(rsync[-2:], ["/tmp/proyecto/", "hercules-cica:slurmscope/qtl/"])

    def test_pull_solo_directorios_permitidos(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.transport.pull(Path(tmp), what=["results", "logs"])
            self.assertEqual(len(self.fake.calls), 2)
            with self.assertRaises(BackendError):
                self.transport.pull(Path(tmp), what=["../.ssh"])


class TestRemoteSlurm(unittest.TestCase):
    def setUp(self):
        self.fake = FakeCluster()
        self.backend = RemoteSlurm(SSHTransport("hercules-cica", "slurmscope/qtl",
                                                runner=self.fake))

    def test_nombre(self):
        self.assertEqual(self.backend.name, "ssh:hercules-cica")

    def test_available_detecta_usuario_remoto(self):
        ok, reason = self.backend.available()
        self.assertTrue(ok, reason)
        self.assertEqual(self.backend.user, "usuario")

    def test_available_sin_slurm(self):
        backend = RemoteSlurm(SSHTransport("h", "d", runner=lambda argv, timeout=None: "yo\n2\n"))
        ok, reason = backend.available()
        self.assertFalse(ok)
        self.assertIn("faltan", reason)

    def test_submit_troceado(self):
        job = self.backend.submit(Path("plan/submit_array.slurm"), 499,
                                  array="1-499%40", env={"SLURMSCOPE_OFFSET": "499"})
        self.assertEqual(job, "5316001")
        remote = self.fake.remote_commands()[-1]
        self.assertIn("--array=1-499%40", remote)
        self.assertIn("--export=ALL,SLURMSCOPE_OFFSET=499", remote)

    def test_variable_de_entorno_invalida(self):
        with self.assertRaises(BackendError):
            RealSlurm.sbatch_args(Path("s"), env={"mal nombre": "1"})
        with self.assertRaises(BackendError):
            RealSlurm.sbatch_args(Path("s"), env={"A": "1,B=2"})


class TestFactoria(unittest.TestCase):
    def test_auto_sin_host_es_slurm_local(self):
        self.assertIsInstance(get_backend("auto", cluster=ClusterConfig()), RealSlurm)
        self.assertNotIsInstance(get_backend("auto", cluster=ClusterConfig()), RemoteSlurm)

    def test_auto_con_host_es_ssh(self):
        cluster = ClusterConfig.from_mapping({"profile": "hercules", "host": "hercules-cica"})
        backend = get_backend("auto", cluster=cluster, project="qtl")
        self.assertIsInstance(backend, RemoteSlurm)
        self.assertEqual(backend.transport.remote_dir, "slurmscope/qtl")

    def test_ssh_sin_host(self):
        with self.assertRaises(BackendError):
            get_backend("ssh", cluster=ClusterConfig())

    def test_desconocido(self):
        with self.assertRaises(BackendError):
            get_backend("pbs")


SPEC_GRANDE = textwrap.dedent(
    """\
    project: grande
    run_id: "{a}/{b}"
    command: "echo {a} {b}"
    sweep:
      a: [{a}]
      b: [{b}]
    cluster:
      profile: hercules
    slurm:
      max_parallel: 40
    """
)


def _big_spec(directory: Path, n_a: int = 40, n_b: int = 30) -> Path:
    body = SPEC_GRANDE.replace("{a}]", ", ".join(str(i) for i in range(n_a)) + "]", 1)
    body = body.replace("{b}]", ", ".join(str(i) for i in range(n_b)) + "]", 1)
    return write_spec(directory, body)


class TestEnvioTroceado(unittest.TestCase):
    """1200 ejecuciones en Hércules (MaxArraySize=500) → 3 job arrays."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(_big_spec(self.dir))
        self.runs = expand_runs(self.spec)
        self.ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.backend = MockSlurm(self.dir / ".slurmscope" / "mock.json", parallel=2000,
                                 min_seconds=10, max_seconds=20)

    def tearDown(self):
        self.tmp.cleanup()

    def test_script_respeta_max_array_size(self):
        script = render_slurm_script(self.spec, self.runs)
        self.assertIn("#SBATCH --array=1-499%40", script)
        self.assertIn("#SBATCH --partition=standard", script)  # del perfil

    def test_tres_envios_y_correspondencia_de_tareas(self):
        artifacts = _generate(self.spec, self.runs, force=True)
        job_ids = submit_sweep(self.spec, self.backend, self.ledger, self.runs, artifacts,
                               log=lambda _m: None)
        self.assertEqual(len(job_ids), 3)
        submissions = sorted(self.ledger.submissions(project="grande"), key=lambda s: s.id)
        self.assertEqual([s.n_runs for s in submissions], [499, 499, 202])

        jobs = self.backend._state["jobs"]
        self.assertEqual(jobs[job_ids[0]]["env"], {})
        self.assertEqual(jobs[job_ids[1]]["env"], {"SLURMSCOPE_OFFSET": "499"})
        self.assertEqual(jobs[job_ids[2]]["array"], "1-202%40")

        # la ejecución 600 es la tarea 101 del segundo array
        record = [r for r in self.ledger.runs(project="grande") if r.run_key == self.runs[599].key]
        self.assertEqual(len(record), 1)
        self.assertEqual(record[0].task_index, 101)
        self.assertEqual(record[0].submission_id, submissions[1].id)

        self.backend.advance(100)
        sync_states(self.ledger, self.backend, project="grande")
        self.assertEqual(self.ledger.counts(project="grande"), {"COMPLETED": 1200})

    def test_la_linea_ejecutada_es_la_correcta(self):
        """Ejecuta de verdad el script con bash para tareas de distintos trozos."""
        import subprocess

        artifacts = _generate(self.spec, self.runs, force=True)
        lines = artifacts["commands"].read_text().splitlines()
        for task, offset in ((1, 0), (101, 499), (202, 998)):
            env = dict(os.environ, SLURM_ARRAY_TASK_ID=str(task), SLURM_ARRAY_JOB_ID="1",
                       SLURMSCOPE_OFFSET=str(offset), SLURM_SUBMIT_DIR=str(self.dir))
            out = subprocess.run(["bash", str(artifacts["script"])], env=env,
                                 capture_output=True, text=True, check=True).stdout
            expected = shlex.split(lines[task + offset - 1].split("&&", 1)[1])[1:]
            self.assertIn(" ".join(expected), out, (task, offset))
            self.assertIn(f"run={task + offset}", out)


class TestReglasDeEnvio(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.cwd = Path.cwd()

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def _cli(self, *argv):
        buffer = io.StringIO()
        with redirect_stdout(buffer), redirect_stderr(buffer):
            code = main(["-C", str(self.dir), *argv])
        return code, buffer.getvalue()

    def test_marenostrum_sin_cuenta_no_se_envia(self):
        write_spec(self.dir, 'project: mn\ncommand: "echo {a}"\nsweep:\n  a: [1, 2]\n'
                             "cluster:\n  profile: marenostrum5-gpp\n")
        code, out = self._cli("submit", "--backend", "slurm", "-y")
        self.assertEqual(code, 2)
        self.assertIn("exige una cuenta", out)
        self.assertIn("exige una QoS", out)

    def test_marenostrum_con_cuenta_y_qos(self):
        write_spec(self.dir, 'project: mn\ncommand: "echo {a}"\nsweep:\n  a: [1, 2]\n'
                             "cluster:\n  profile: marenostrum5-gpp\n"
                             "slurm:\n  account: upo99\n  qos: gp_resb\n")
        code, out = self._cli("submit", "--backend", "slurm", "--dry-run")
        self.assertEqual(code, 0, out)
        script = (self.dir / "plan" / "submit_array.slurm").read_text()
        self.assertIn("#SBATCH --account=upo99", script)
        self.assertIn("#SBATCH --qos=gp_resb", script)
        self.assertNotIn("--partition", script)  # la decide la QoS

    def test_inyeccion_en_directivas(self):
        write_spec(self.dir, 'project: x\ncommand: "echo {a}"\nsweep:\n  a: [1]\n'
                             'slurm:\n  account: "a\\n#SBATCH --uid=0"\n')
        code, out = self._cli("plan")
        self.assertEqual(code, 2)
        self.assertIn("slurm.account", out)

    def test_dry_run_muestra_los_trozos(self):
        _big_spec(self.dir)
        code, out = self._cli("submit", "--backend", "slurm", "--dry-run")
        self.assertEqual(code, 0, out)
        self.assertEqual(out.count("sbatch --parsable"), 3)
        self.assertIn("SLURMSCOPE_OFFSET=998", out)
        self.assertIn("ejecuciones 999-1200", out)

    def test_clusters_lista_perfiles(self):
        code, out = self._cli("clusters")
        self.assertEqual(code, 0)
        for name in ("hercules", "marenostrum5-gpp", "finisterrae3"):
            self.assertIn(name, out)
        code, out = self._cli("clusters", "--name", "hercules")
        self.assertIn("VPN", out)


class TestRecuentoConReenvios(unittest.TestCase):
    def test_relanzar_no_duplica(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            spec = load_spec(write_spec(directory))
            runs = expand_runs(spec)
            ledger = Ledger(directory / "l.db")
            backend = MockSlurm(directory / "m.json", parallel=100, failure_rate=1.0,
                                min_seconds=1, max_seconds=2)
            artifacts = _generate(spec, runs, force=True)
            submit_sweep(spec, backend, ledger, runs, artifacts, log=lambda _m: None)
            backend.advance(10)
            sync_states(ledger, backend, project=spec.project)
            self.assertEqual(ledger.counts(project=spec.project), {"FAILED": 8})

            backend.failure_rate = 0.0
            submit_sweep(spec, backend, ledger, runs, artifacts, log=lambda _m: None)
            backend.advance(10)
            sync_states(ledger, backend, project=spec.project)
            # 8 ejecuciones, no 16: cuenta el último intento de cada una
            self.assertEqual(ledger.counts(project=spec.project), {"COMPLETED": 8})


if __name__ == "__main__":
    unittest.main()
