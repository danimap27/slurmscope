"""Normas de los centros: comprobación previa al envío, script y envío por oleadas."""

from __future__ import annotations

import io
import json
import os
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.backends import SSHTransport  # noqa: E402
from slurmscope.cli import main  # noqa: E402
from slurmscope.clusters import (  # noqa: E402
    PROFILES,
    compress_indices,
    parse_array_indices,
    parse_qos_table,
    plan_batches,
)
from slurmscope.config import ConfigError, load_spec  # noqa: E402
from slurmscope.ledger import Ledger  # noqa: E402
from slurmscope.matrix import expand_runs, render_slurm_script  # noqa: E402
from slurmscope.rules import (  # noqa: E402
    ERROR,
    INFO,
    WARNING,
    check_spec,
    effective_limits,
    slurm_time_seconds,
    submit_capacity,
)


def spec_text(profile: str, slurm: str = "", cluster: str = "", n: int = 4) -> str:
    values = ", ".join(str(i) for i in range(1, n + 1))
    return textwrap.dedent(f"""\
        project: normas
        command: "echo {{a}}"
        sweep:
          a: [{values}]
        cluster:
          profile: {profile}
        """) + textwrap.indent(cluster, "  ") + "slurm:\n" + textwrap.indent(
        slurm or 'time: "01:00:00"\n', "  ")


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.cwd = Path.cwd()

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def spec(self, *args, **kwargs):
        return load_spec(write_spec(self.dir, spec_text(*args, **kwargs)))

    def codes(self, spec, level=None):
        findings = check_spec(spec, len(expand_runs(spec)))
        return {f.code for f in findings if level is None or f.level == level}

    def cli(self, *argv):
        buffer = io.StringIO()
        with redirect_stdout(buffer), redirect_stderr(buffer):
            code = main(["-C", str(self.dir), *argv])
        return code, buffer.getvalue()


class TestTiempos(unittest.TestCase):
    def test_formatos_de_slurm(self):
        cases = {"30": 1800, "10:30": 630, "01:00:00": 3600, "2-00:00:00": 172800,
                 "1-12": 129600, "3-04:30": 275400, "UNLIMITED": None, "basura": None}
        for text, expected in cases.items():
            self.assertEqual(slurm_time_seconds(text), expected, text)


class TestPerfilesDocumentados(unittest.TestCase):
    def test_todos_los_centros(self):
        for name in ("hercules", "picasso", "albaicin", "marenostrum5-gpp", "marenostrum5-acc",
                     "finisterrae3", "calendula", "tirant", "altamira", "lapalma", "teidehpc",
                     "pirineus3", "agustina", "cibeles", "lusitania", "urederra", "xula",
                     "turgalium", "dipc-hyperion", "generic"):
            self.assertIn(name, PROFILES)

    def test_cada_dato_tiene_fuente(self):
        for profile in PROFILES.values():
            if profile.name != "generic":
                self.assertTrue(profile.docs, profile.name)
                self.assertTrue(all(d.startswith("https://") for d in profile.docs))

    def test_limites_validos(self):
        for profile in PROFILES.values():
            for name, limits in {**profile.qos_limits, **profile.partition_limits}.items():
                if limits.max_wall:
                    self.assertIsNotNone(slurm_time_seconds(limits.max_wall),
                                         f"{profile.name}/{name}")
            for tier in profile.time_tiers:
                self.assertIn(tier, {**profile.qos_limits, **profile.partition_limits})


class TestComprobaciones(Base):
    def test_marenostrum_cuenta_y_qos(self):
        self.assertEqual(self.codes(self.spec("marenostrum5-gpp"), ERROR), {"account", "qos"})
        ok = self.spec("marenostrum5-gpp", 'time: "01:00:00"\naccount: upo1\nqos: gp_resb\n')
        self.assertEqual(self.codes(ok, ERROR), set())

    def test_tiempo_mayor_que_la_qos(self):
        spec = self.spec("marenostrum5-gpp", 'time: "3-00:00:00"\naccount: a\nqos: gp_resb\n')
        self.assertIn("time-limit", self.codes(spec, ERROR))

    def test_hercules_siete_dias(self):
        self.assertIn("time-limit", self.codes(self.spec("hercules", 'time: "8-00:00:00"\n'), ERROR))
        self.assertNotIn("time-limit", self.codes(self.spec("hercules", 'time: "6-00:00:00"\n')))

    def test_finisterrae_asigna_qos_por_tiempo(self):
        spec = self.spec("finisterrae3", 'time: "10:00:00"\n', n=120)
        name, limits = effective_limits(spec)
        self.assertEqual(name, "medium")  # > 6 h y <= 3 días
        self.assertEqual(limits.max_submit, 50)
        self.assertIn("waves", self.codes(spec, INFO))
        self.assertIsNone(spec.effective_qos)

    def test_finisterrae_qos_desconocida(self):
        spec = self.spec("finisterrae3", 'time: "01:00:00"\nqos: gp_resb\n')
        self.assertIn("qos-auto", self.codes(spec, WARNING))

    def test_finisterrae_class_c_paralelismo(self):
        spec = self.spec("finisterrae3", 'time: "01:00:00"\nqos: class_c\nmax_parallel: 4\n')
        self.assertIn("parallel", self.codes(spec, WARNING))
        self.assertEqual(submit_capacity(spec), 5)

    def test_cpu_por_gpu_en_marenostrum(self):
        base = 'time: "01:00:00"\naccount: a\nqos: acc_resb\nextra_sbatch: ["#SBATCH --gres=gpu:2"]\n'
        self.assertIn("cpus-per-gpu",
                      self.codes(self.spec("marenostrum5-acc", base + "cpus_per_task: 20\n"), ERROR))
        self.assertNotIn("cpus-per-gpu",
                         self.codes(self.spec("marenostrum5-acc", base + "cpus_per_task: 40\n")))

    def test_sin_internet(self):
        spec = self.spec("calendula", 'time: "01:00:00"\nsetup: "pip install torch"\n')
        self.assertIn("internet", self.codes(spec, ERROR))
        spec = self.spec("hercules", 'time: "01:00:00"\nsetup: "pip install torch"\n')
        self.assertNotIn("internet", self.codes(spec))  # no documentado: no se presupone

    def test_picasso_arrays_grandes(self):
        self.assertIn("support", self.codes(self.spec("picasso", n=1500), WARNING))

    def test_directorio_de_trabajo_en_home(self):
        cluster = "host: glogin1.bsc.es\n"
        slurm = 'time: "01:00:00"\naccount: a\nqos: gp_resb\n'
        self.assertIn("workdir", self.codes(self.spec("marenostrum5-gpp", slurm, cluster)))
        cluster += 'remote_dir: "/gpfs/scratch/grupo/{project}"\n'
        self.assertNotIn("workdir", self.codes(self.spec("marenostrum5-gpp", slurm, cluster)))

    def test_login_no_documentado(self):
        spec = self.spec("hercules", cluster="host: usuario@otro.ejemplo.es\n")
        self.assertIn("login", self.codes(spec, INFO))

    def test_probe_manda_sobre_el_perfil(self):
        spec = self.spec("generic", 'time: "01:00:00"\nqos: normal\n', n=30)
        probe = {"max_array_size": 10, "qos": {"normal": {"max_wall": "00:30:00",
                                                          "max_jobs": 2, "max_submit": 8}}}
        codes = {f.code for f in check_spec(spec, 30, probe) if f.level == ERROR}
        self.assertEqual(codes, {"array-size", "time-limit"})
        self.assertEqual(submit_capacity(spec, probe), 8)


class TestScriptSegunElCentro(Base):
    def test_altamira_exige_export_none(self):
        script = render_slurm_script(spec := self.spec("altamira"), expand_runs(spec))
        self.assertIn("#SBATCH --export=NONE", script)
        self.assertIn("source /etc/profile.d/modules.sh", script)

    def test_picasso_constraint_por_defecto_y_sustituible(self):
        spec = self.spec("picasso")
        self.assertIn("#SBATCH --constraint=cal", render_slurm_script(spec, expand_runs(spec)))
        spec = self.spec("picasso", 'time: "01:00:00"\nextra_sbatch: ["#SBATCH --constraint=dgx"]\n')
        script = render_slurm_script(spec, expand_runs(spec))
        self.assertIn("--constraint=dgx", script)
        self.assertNotIn("--constraint=cal", script)

    def test_horas_res(self):
        spec = self.spec("calendula", cluster="allocation: res\n")
        script = render_slurm_script(spec, expand_runs(spec))
        self.assertIn("#SBATCH --partition=res", script)
        self.assertIn("#SBATCH --qos=class_a", script)
        # FinisTerrae III asigna la QoS: con horas RES tampoco se fuerza
        spec = self.spec("finisterrae3", cluster="allocation: res\n")
        self.assertNotIn("--qos", render_slurm_script(spec, expand_runs(spec)))

    def test_hilos_y_correo(self):
        spec = self.spec("generic", 'time: "01:00:00"\nmail_user: yo@upo.es\nmail_type: "end,fail"\n')
        script = render_slurm_script(spec, expand_runs(spec))
        self.assertIn('export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"', script)
        self.assertIn("#SBATCH --mail-user=yo@upo.es", script)
        self.assertIn("#SBATCH --mail-type=END,FAIL", script)

    def test_correo_invalido(self):
        with self.assertRaises(ConfigError):
            self.spec("generic", 'time: "01:00:00"\nmail_type: SIEMPRE\n')
        with self.assertRaises(ConfigError):
            self.spec("generic", 'time: "01:00:00"\nmail_user: "a@b.es,c@d.es"\n')

    def test_allocation_invalida(self):
        with self.assertRaises(ConfigError):
            self.spec("generic", cluster="allocation: gratis\n")


class TestAvance(Base):
    def test_resultados_antiguos_no_adelantan_un_envio_nuevo(self):
        from _helpers import write_result
        from slurmscope.report import build_dashboard

        spec = self.spec("generic")
        runs = expand_runs(spec)
        for run in runs:  # resultados de un barrido anterior
            write_result(self.dir, run.run_dir, {"metrics": {"acc": 0.9}})
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.assertEqual(build_dashboard(spec, ledger, sync=False).progress, 100.0)
        ledger.record_submission("normas", "mock", len(runs), runs, array_job_id="1")
        self.assertEqual(build_dashboard(spec, ledger, sync=False).progress, 0.0)


class TestIndices(unittest.TestCase):
    def test_comprimir_y_leer(self):
        self.assertEqual(compress_indices([9, 1, 2, 3, 7, 10]), "1-3,7,9-10")
        self.assertEqual(parse_array_indices("1-3,7,9-12:2%4"), [1, 2, 3, 7, 9, 11])
        for values in ([1], [1, 3, 5], list(range(1, 500))):
            self.assertEqual(parse_array_indices(compress_indices(values)), values)

    def test_lotes_respetan_max_array_size(self):
        batches = plan_batches([1, 2, 498, 499, 500, 1000, 1200], 500)
        self.assertEqual([b.offset for b in batches], [0, 499, 998])
        self.assertEqual(batches[0].indices, (1, 2, 498, 499))
        self.assertEqual(batches[1].tasks, [1])            # ejecución 500
        self.assertEqual(batches[2].tasks, [2, 202])       # ejecuciones 1000 y 1200
        for batch in batches:
            self.assertTrue(all(1 <= t <= 499 for t in batch.tasks))
        self.assertEqual(batches[2].array_spec(10), "2,202%10")

    def test_tabla_de_qos(self):
        table = parse_qos_table("normal|2-00:00:00|100|500\ndebug|00:30:00||\n")
        self.assertEqual(table["normal"], {"max_wall": "2-00:00:00", "max_jobs": 100,
                                           "max_submit": 500})
        self.assertIsNone(table["debug"]["max_submit"])


class TestOleadas(Base):
    """FinisTerrae III con class_b (MaxSubmit=15): 40 ejecuciones van en 3 oleadas."""

    def test_submit_y_resume(self):
        write_spec(self.dir, spec_text("finisterrae3", 'time: "01:00:00"\nqos: class_b\n'
                                       "max_parallel: 3\n", n=40))
        mock = ["--backend", "mock", "--mock-parallel", "50"]
        code, out = self.cli(*mock, "submit", "-y")
        self.assertEqual(code, 0, out)
        self.assertIn("quedan 25", out)
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.assertEqual(sum(ledger.counts(project="normas").values()), 15)

        code, out = self.cli(*mock, "resume", "-y")  # la cola sigue llena
        self.assertEqual(code, 1)
        self.assertIn("cola está llena", out)

        self.cli(*mock, "mock", "--advance", "100000")
        code, out = self.cli(*mock, "resume", "-y")
        self.assertEqual(code, 0, out)
        self.assertIn("enviadas 15", out)
        self.cli(*mock, "mock", "--advance", "100000")
        code, out = self.cli(*mock, "resume", "-y")
        self.assertIn("enviadas 10", out)
        self.cli(*mock, "mock", "--advance", "100000")
        code, out = self.cli(*mock, "resume")
        self.assertIn("no hay nada pendiente", out)
        self.assertEqual(ledger.counts(project="normas"), {"COMPLETED": 40})

    def test_relanzar_fallidas(self):
        write_spec(self.dir, spec_text("generic", 'time: "01:00:00"\n', n=10))
        mock = ["--backend", "mock", "--mock-parallel", "50"]
        self.cli(*mock, "--fail-rate", "1.0", "submit", "-y")
        self.cli(*mock, "mock", "--advance", "100000")
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.assertEqual(ledger.counts(project="normas"), {"FAILED": 10})
        code, out = self.cli(*mock, "resume")
        self.assertIn("--failed", out)
        code, out = self.cli(*mock, "resume", "--failed", "-y")
        self.assertEqual(code, 0, out)
        self.cli(*mock, "mock", "--advance", "100000")
        self.assertEqual(ledger.counts(project="normas"), {"COMPLETED": 10})

    def test_check_por_cli(self):
        write_spec(self.dir, spec_text("marenostrum5-gpp"))
        code, out = self.cli("check")
        self.assertEqual(code, 1)
        self.assertIn("MareNostrum 5", out)
        code, out = self.cli("check", "--json")
        self.assertEqual({f["code"] for f in json.loads(out)} >= {"account", "qos"}, True)

    def test_agradecimiento(self):
        code, out = self.cli("acknowledge", "--name", "lapalma")
        self.assertEqual(code, 0)
        self.assertIn("LaPalma Supercomputer", out)
        self.assertIn("RES Intranet", out)


class TestRemoteDirConVariables(unittest.TestCase):
    def test_se_resuelve_en_el_cluster_una_sola_vez(self):
        calls = []

        def runner(argv, timeout=None):
            calls.append(argv[-1])
            return "/mnt/lustre/scratch/nlsas/home/upo/x/slurmscope/p\n" if "printf" in argv[-1] else ""

        transport = SSHTransport("ft3.cesga.es", remote_dir="$LUSTRE/slurmscope/p", runner=runner)
        self.assertEqual(transport.remote_dir, "/mnt/lustre/scratch/nlsas/home/upo/x/slurmscope/p")
        transport.run(["squeue"])
        self.assertEqual(sum("printf" in c for c in calls), 1)
        self.assertTrue(calls[-1].startswith("cd /mnt/lustre/"))

    def test_respuesta_no_valida(self):
        from slurmscope.backends import BackendError

        for answer in ("", "relativa/x", "$LUSTRE/x"):
            transport = SSHTransport("h", remote_dir="$LUSTRE/x",
                                     runner=lambda a, timeout=None, out=answer: out)
            with self.assertRaises(BackendError, msg=answer):
                _ = transport.remote_dir


if __name__ == "__main__":
    unittest.main()
