"""Contabilidad detallada (sacct), eficiencia de recursos, ``tune`` y ``watch``."""

from __future__ import annotations

import io
import json
import os
import sqlite3
import sys
import tempfile
import textwrap
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.backends import (  # noqa: E402
    MockSlurm,
    parse_cpu_time,
    parse_sacct,
    parse_slurm_memory,
    parse_timelimit,
)
from slurmscope.cli import main  # noqa: E402
from slurmscope.config import load_spec  # noqa: E402
from slurmscope.efficiency import analyse, format_mem, format_time  # noqa: E402
from slurmscope.ledger import Ledger, RunRecord  # noqa: E402

# Salida real de sacct (formato de RealSlurm.ACCOUNTING_FORMAT), con pasos.
SACCT = (
    "5355721_1|ss|COMPLETED|0:0|00:00:08|2026-10-17T08:45:34|2026-10-17T08:45:42|00:05:00|1|512M||00:00.012\n"
    "5355721_1.batch|batch|COMPLETED|0:0|00:00:08|2026-10-17T08:45:34|2026-10-17T08:45:42||1||3400K|00:00.010\n"
    "5355721_1.extern|extern|COMPLETED|0:0|00:00:08|2026-10-17T08:45:34|2026-10-17T08:45:42||1||900K|00:00.002\n"
    "5355721_2|ss|TIMEOUT|0:15|00:05:01|2026-10-17T08:45:34|2026-10-17T08:50:35|00:05:00|4|2Gc||\n"
    "5355721_2.batch|batch|CANCELLED|0:15|00:05:01|2026-10-17T08:45:34|2026-10-17T08:50:35||4||1.5G|19:30.500\n"
    "5355721_[3-6]|ss|PENDING|0:0|00:00:00|Unknown|Unknown|00:05:00|1|512M||\n"
)


class TestParsers(unittest.TestCase):
    def test_memoria(self):
        self.assertEqual(parse_slurm_memory("4G"), 4096.0)
        self.assertEqual(parse_slurm_memory("4000M"), 4000.0)
        self.assertEqual(parse_slurm_memory("1024K"), 1.0)
        self.assertEqual(parse_slurm_memory("4Gn"), 4096.0)
        self.assertEqual(parse_slurm_memory("2Gc", cpus=4), 8192.0)
        self.assertIsNone(parse_slurm_memory(""))
        self.assertIsNone(parse_slurm_memory("mucha"))

    def test_tiempo_de_cpu(self):
        self.assertEqual(parse_cpu_time("00:00.012"), 0.012)
        self.assertEqual(parse_cpu_time("19:30.500"), 1170.5)
        self.assertEqual(parse_cpu_time("01:02:03"), 3723.0)
        self.assertEqual(parse_cpu_time("2-00:00:00"), 172800.0)
        self.assertIsNone(parse_cpu_time(""))

    def test_timelimit(self):
        self.assertEqual(parse_timelimit("00:05:00"), 300.0)
        self.assertEqual(parse_timelimit("7-00:00:00"), 604800.0)
        self.assertIsNone(parse_timelimit("UNLIMITED"))

    def test_sacct_con_pasos(self):
        entries = {e.job_id: e for e in parse_sacct(SACCT)}
        self.assertEqual(set(entries), {"5355721_1", "5355721_2", "5355721_[3-6]"})
        first = entries["5355721_1"]
        self.assertEqual(first.state, "COMPLETED")
        self.assertAlmostEqual(first.max_rss_mb, 3400 / 1024, places=2)  # máximo de los pasos
        self.assertEqual(first.cpu_seconds, 0.012)                     # la línea principal
        self.assertEqual(first.timelimit_seconds, 300.0)
        self.assertEqual(first.req_mem_mb, 512.0)
        second = entries["5355721_2"]
        self.assertEqual((second.state, second.raw_state), ("FAILED", "TIMEOUT"))
        self.assertEqual(second.req_mem_mb, 8192.0)                     # 2G por CPU × 4
        self.assertEqual(second.cpu_seconds, 1170.5)                    # suma de pasos
        self.assertEqual(second.max_rss_mb, 1536.0)
        self.assertIsNone(entries["5355721_[3-6]"].task_index)

    def test_formato_antiguo_de_siete_columnas(self):
        old = "5316479_1|qtl|COMPLETED|0:0|01:20:15|2026-10-16T10:00:00|2026-10-16T11:20:15\n"
        entry = parse_sacct(old)[0]
        self.assertEqual(entry.seconds, 4815.0)
        self.assertIsNone(entry.max_rss_mb)


class TestLibro(unittest.TestCase):
    def test_migra_un_libro_antiguo(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ledger.db"
            conn = sqlite3.connect(path)
            conn.executescript(
                "CREATE TABLE runs (id INTEGER PRIMARY KEY, submission_id INTEGER, "
                "task_index INTEGER, run_key TEXT, run_dir TEXT, params TEXT, state TEXT, "
                "exit_code INTEGER, slurm_job_id TEXT, started_at TEXT, ended_at TEXT, "
                "seconds REAL, updated_at TEXT);")
            conn.close()
            Ledger(path)
            columns = {row[1] for row in sqlite3.connect(path).execute("PRAGMA table_info(runs)")}
            for name in ("raw_state", "max_rss_mb", "cpu_seconds", "timelimit_seconds"):
                self.assertIn(name, columns)

    def test_causas(self):
        base = dict(id=1, submission_id=1, task_index=1, run_key="k", run_dir=None, params={},
                    slurm_job_id=None, started_at=None, ended_at=None, seconds=10.0)
        cases = [("FAILED", "TIMEOUT", 0, "tiempo agotado"),
                 ("FAILED", "OUT_OF_MEMORY", 0, "memoria agotada"),
                 ("FAILED", "NODE_FAIL", 0, "fallo del nodo"),
                 ("FAILED", "FAILED", 2, "código de salida 2"),
                 ("CANCELLED", "CANCELLED", None, "cancelado"),
                 ("COMPLETED", "COMPLETED", 0, None)]
        for state, raw, code, cause in cases:
            record = RunRecord(state=state, raw_state=raw, exit_code=code, **base)
            self.assertEqual(record.cause, cause, raw)


def _record(i, seconds, state="COMPLETED", raw=None, rss=1000.0, cpu_ratio=0.9):
    return RunRecord(id=i, submission_id=1, task_index=i, run_key=f"r{i}", run_dir=None,
                     params={}, state=state, exit_code=0, slurm_job_id=None, started_at=None,
                     ended_at=None, seconds=seconds, raw_state=raw or state,
                     timelimit_seconds=8 * 3600.0, alloc_cpus=4, req_mem_mb=16384.0,
                     max_rss_mb=rss, cpu_seconds=seconds * 4 * cpu_ratio)


class TestEficiencia(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.spec = load_spec(write_spec(self.dir, textwrap.dedent("""\
            project: ef
            command: "echo {a}"
            sweep:
              a: [1, 2, 3, 4]
            slurm:
              time: "08:00:00"
              mem: 16G
              cpus_per_task: 4
            """)))

    def tearDown(self):
        self.tmp.cleanup()

    def test_recursos_sobredimensionados(self):
        records = [_record(i, 1800 + 60 * i, cpu_ratio=0.3) for i in range(10)]
        report = analyse(records, self.spec, max_wall_seconds=7 * 86400)
        proposals = {s.option: s.proposed for s in report.suggestions}
        self.assertEqual(proposals["time"], "01:00:00")   # p95 2340 s ×1.25 → 15 min
        self.assertEqual(proposals["mem"], "2G")          # 1000 MB + 1 GB de margen
        self.assertEqual(proposals["cpus_per_task"], "2")  # 30 % de 4 CPU
        self.assertAlmostEqual(report.cpu_efficiency_median, 0.3)
        self.assertGreater(report.wasted_core_hours, 0)

    def test_bien_ajustado_no_propone_nada(self):
        records = [_record(i, 6 * 3600, rss=12000.0) for i in range(5)]
        self.assertEqual(analyse(records, self.spec).suggestions, [])

    def test_timeouts_y_oom_suben_recursos(self):
        records = [_record(1, 3600), _record(2, 28800, "FAILED", "TIMEOUT"),
                   _record(3, 100, "FAILED", "OUT_OF_MEMORY")]
        report = analyse(records, self.spec, max_wall_seconds=10 * 3600)
        proposals = {s.option: s.proposed for s in report.suggestions}
        self.assertEqual(proposals["time"], "10:00:00")  # 12 h limitado al máximo
        self.assertEqual(proposals["mem"], "32G")
        self.assertEqual(report.causes, {"tiempo agotado": 1, "memoria agotada": 1})

    def test_formatos(self):
        self.assertEqual(format_time(93784), "1-02:03:04")
        self.assertEqual(format_mem(1536), "2G")
        self.assertEqual(format_mem(300), "300M")


class TestSimuladorConUso(unittest.TestCase):
    def test_contabilidad_simulada(self):
        with tempfile.TemporaryDirectory() as tmp:
            script = Path(tmp) / "s.slurm"
            script.write_text("#!/bin/bash\n#SBATCH --cpus-per-task=8\n#SBATCH --mem=4G\n"
                              "#SBATCH --time=02:00:00\n", encoding="utf-8")
            backend = MockSlurm(Path(tmp) / "m.json", parallel=50, failure_rate=0.5)
            job = backend.submit(script, 40)
            backend.advance(10_000)
            entries = backend.accounting([job])
            done = [e for e in entries if e.state == "COMPLETED"]
            self.assertTrue(done)
            self.assertTrue(all(e.alloc_cpus == 8 and e.req_mem_mb == 4096 for e in entries))
            self.assertTrue(all(0 < e.max_rss_mb < 4096 for e in done))
            self.assertTrue(all(0 < e.cpu_seconds <= e.seconds * 8 for e in done))
            causes = {e.raw_state for e in entries if e.state == "FAILED"}
            self.assertTrue(causes <= {"FAILED", "TIMEOUT", "OUT_OF_MEMORY"})

    def test_duraciones_no_cambian(self):
        """Añadir el uso simulado no altera las duraciones deterministas."""
        with tempfile.TemporaryDirectory() as tmp:
            backend = MockSlurm(Path(tmp) / "m.json")
            job = backend.submit(Path("noexiste.slurm"), 3)
            durations = [t["duration"] for t in backend._state["jobs"][job]["tasks"].values()]
            self.assertEqual(len(set(durations)), 3)


class TestComandos(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)
        self.cwd = Path.cwd()
        write_spec(self.dir, textwrap.dedent("""\
            project: vigilado
            command: "echo {a}"
            sweep:
              a: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
            cluster:
              profile: finisterrae3
            slurm:
              time: "01:00:00"
              qos: class_c
              mem: 16G
              max_parallel: 1
            """))
        self.mock = ["-C", str(self.dir), "--backend", "mock", "--mock-parallel", "50"]

    def tearDown(self):
        os.chdir(self.cwd)
        self.tmp.cleanup()

    def cli(self, *argv):
        buffer = io.StringIO()
        with redirect_stdout(buffer), redirect_stderr(buffer):
            code = main([*self.mock, *argv])
        return code, buffer.getvalue()

    def test_watch_lleva_el_barrido_al_final_por_oleadas(self):
        # class_c: MaxSubmit=5 → 20 ejecuciones en 4 oleadas, sin intervención
        code, out = self.cli("watch", "--interval", "100000")
        self.assertEqual(code, 0, out)
        self.assertIn("barrido terminado: 20 completadas, 0 fallidas", out)
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        self.assertEqual(len(ledger.submissions(project="vigilado")), 4)

    def test_watch_reintenta_solo_lo_transitorio(self):
        code, out = self.cli("--fail-rate", "1.0", "watch", "--interval", "100000",
                             "--retry", "2")
        self.assertEqual(code, 1)
        ledger = Ledger(self.dir / ".slurmscope" / "ledger.db")
        attempts = ledger.attempts(project="vigilado")
        latest = ledger.latest(project="vigilado")
        for key, record in latest.items():
            if record.cause in ("tiempo agotado", "memoria agotada"):
                self.assertLessEqual(attempts[key], 3)
            else:
                self.assertEqual(attempts[key], 3, key)  # 1 + 2 reintentos
        self.assertIn("slurmscope tune", out)

    def test_watch_una_vuelta(self):
        code, out = self.cli("watch", "--once")
        self.assertEqual(code, 0)
        self.assertIn("vuelta 1", out)
        self.assertIn("enviadas 5", out)

    def test_tune(self):
        self.cli("watch", "--interval", "100000")
        code, out = self.cli("tune")
        self.assertEqual(code, 0, out)
        self.assertIn("eficiencia de CPU", out)
        code, out = self.cli("tune", "--json")
        data = json.loads(out)
        self.assertEqual(data["n_completed"], 20)
        self.assertIsNotNone(data["cpu_efficiency_median"])

    def test_informe_incluye_eficiencia_y_normas(self):
        self.cli("watch", "--interval", "100000")
        code, out = self.cli("report")
        self.assertEqual(code, 0, out)
        md = next((self.dir / "reports").glob("*-informe.md")).read_text(encoding="utf-8")
        self.assertIn("## Eficiencia de recursos", md)
        self.assertIn("FinisTerrae III", md)
        state = json.loads(next((self.dir / "reports").glob("*-estado.json")).read_text())
        self.assertIn("efficiency", state)
        self.assertIn("findings", state)


if __name__ == "__main__":
    unittest.main()
