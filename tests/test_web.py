"""Panel web: rutas, solo lectura y modo demostración."""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _helpers import write_spec  # noqa: E402
from slurmscope.web import DEMO_MARKER, Panel, Project, make_handler  # noqa: E402


class _Server:
    def __init__(self, panel: Panel):
        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(panel))
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def get(self, path: str):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}{path}", timeout=5) as r:
            return r.status, r.headers, r.read().decode("utf-8")

    def close(self):
        self.httpd.shutdown()
        self.httpd.server_close()


class TestPanelWeb(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name) / "qtl-demo"
        self.dir.mkdir()
        write_spec(self.dir)
        (self.dir / DEMO_MARKER).write_text("demo\n", encoding="utf-8")
        self.project = Project(self.dir, backend_name="mock")
        self.panel = Panel([self.project], sync_interval=0, demo_tick=600)
        self.server = _Server(self.panel)

    def tearDown(self):
        self.server.close()
        self.tmp.cleanup()

    def test_pagina_y_cabeceras_de_seguridad(self):
        self.panel.refresh_all()
        status, headers, body = self.server.get("/")
        self.assertEqual(status, 200)
        self.assertIn("SlurmScope", body)
        self.assertIn("default-src 'none'", headers["Content-Security-Policy"])
        self.assertEqual(headers["X-Content-Type-Options"], "nosniff")

    def test_ciclo_demo_y_api(self):
        self.panel.refresh_all()  # primera vuelta: envía el barrido
        _, _, body = self.server.get("/api/projects")
        summary = json.loads(body)
        self.assertEqual(summary["projects"][0]["slug"], "qtl-demo")
        self.assertEqual(summary["projects"][0]["total"], 8)

        for _ in range(6):  # avanza la simulación hasta terminar
            self.panel.refresh_all()
        _, _, body = self.server.get("/api/project/qtl-demo")
        data = json.loads(body)
        self.assertTrue(data["demo"])
        self.assertEqual(data["total"], 8)
        self.assertEqual(sum(data["counts"].values()), 8)
        self.assertEqual(data["cluster"]["profile"], "generic")
        states = {r["state"] for r in data["runs"]}
        self.assertTrue(states <= {"COMPLETED", "FAILED"}, states)
        self.assertGreater(data["on_disk"], 0)  # el modo demo escribe métricas
        self.assertIn("accuracy", data["metrics"])

        _, headers, csv_text = self.server.get("/api/project/qtl-demo/runs.csv")
        self.assertIn("text/csv", headers["Content-Type"])
        self.assertEqual(len(csv_text.strip().splitlines()), 9)  # cabecera + 8

    def test_rutas_desconocidas_y_solo_lectura(self):
        for path in ("/api/project/otro", "/../../etc/passwd", "/api/submit"):
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                self.server.get(path)
            self.assertEqual(ctx.exception.code, 404)
            ctx.exception.close()
        request = urllib.request.Request(f"http://127.0.0.1:{self.server.port}/api/projects",
                                         data=b"{}", method="POST")
        with self.assertRaises(urllib.error.HTTPError) as ctx:
            urllib.request.urlopen(request, timeout=5)
        self.assertEqual(ctx.exception.code, 501)  # no hay ninguna acción de escritura
        ctx.exception.close()

    def test_healthz(self):
        _, _, body = self.server.get("/healthz")
        self.assertTrue(json.loads(body)["ok"])


class TestSincronizacionPrudente(unittest.TestCase):
    """Un clúster real no se consulta sin necesidad."""

    def test_no_sincroniza_sin_tareas_activas_ni_antes_de_tiempo(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "real"
            directory.mkdir()
            write_spec(directory)
            project = Project(directory, backend_name="slurm")
            self.assertEqual(project.min_interval, 120.0)
            self.assertTrue(project.should_sync(0))  # primera vez, siempre
            project.last_sync = 1000.0
            self.assertFalse(project.should_sync(5000.0))  # nada pendiente

            from slurmscope.matrix import expand_runs
            project.ledger.record_submission("prueba", "slurm", 8, expand_runs(project.spec),
                                             array_job_id="1")
            self.assertFalse(project.should_sync(1060.0))  # demasiado pronto
            self.assertTrue(project.should_sync(1121.0))

    def test_demo_siempre_usa_el_simulador(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "demo"
            directory.mkdir()
            write_spec(directory, 'project: d\ncommand: "echo {a}"\nsweep:\n  a: [1]\n'
                                  "cluster:\n  profile: hercules\n  host: hercules-cica\n")
            (directory / DEMO_MARKER).write_text("", encoding="utf-8")
            self.assertEqual(Project(directory).backend.name, "mock")


class TestModoDemoProtegido(unittest.TestCase):
    def test_sin_marcador_no_inventa_resultados_ni_borra(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp) / "real"
            directory.mkdir()
            write_spec(directory)
            keep = directory / "results" / "conservar.txt"
            keep.parent.mkdir(parents=True)
            keep.write_text("no tocar", encoding="utf-8")

            project = Project(directory, backend_name="mock")
            panel = Panel([project], sync_interval=0, demo_tick=10_000)
            for _ in range(12):
                panel.refresh_all()
            self.assertFalse(project.snapshot["demo"])
            self.assertTrue(keep.is_file())
            self.assertEqual(project.cycle, 1)
            metrics = list((directory / "results").rglob("metrics.json"))
            self.assertEqual(metrics, [])


if __name__ == "__main__":
    unittest.main()
