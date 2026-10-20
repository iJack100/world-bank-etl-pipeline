import copy
import csv
import json
from pathlib import Path
import shutil
import sqlite3
import unittest
from unittest.mock import patch
import uuid

import pipeline


class PipelineTests(unittest.TestCase):
    def setUp(self):
        # mkdir normal evita ACL restrictivas de tempfile en Windows sandbox.
        self.root = pipeline.ROOT / ".test_runs" / uuid.uuid4().hex
        self.root.mkdir(parents=True)
        self.assertEqual(self.root.resolve().parent, (pipeline.ROOT / ".test_runs").resolve())
        self.addCleanup(self.cleanup_test_folder)
        self.raw = self.root / "raw"
        self.output = self.root / "outputs"
        shutil.copytree(pipeline.ROOT / "data" / "raw", self.raw)

    def cleanup_test_folder(self):
        expected_parent = (pipeline.ROOT / ".test_runs").resolve()
        if self.root.resolve().parent != expected_parent:
            raise RuntimeError("Los temporales de prueba deben estar dentro del proyecto")
        shutil.rmtree(self.root)

    def change_payload(self, filename, change):
        path = self.raw / filename
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
        change(payload)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def connect(self):
        conn = sqlite3.connect(self.output / "indicadores.sqlite")
        self.addCleanup(conn.close)
        return conn

    def test_real_snapshots_offline_idempotency(self):
        with patch("pipeline.urlopen", side_effect=AssertionError("No debe acceder a la red")):
            first = pipeline.run_pipeline(self.raw, self.output)
            second = pipeline.run_pipeline(self.raw, self.output)
        self.assertEqual(first["metrics"]["inserted_rows"], 36)
        self.assertEqual(second["metrics"]["inserted_rows"], 0)
        self.assertEqual(second["metrics"]["updated_rows"], 0)
        self.assertEqual(second["metrics"]["unchanged_rows"], 36)
        conn = self.connect()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 36)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM pipeline_runs").fetchone()[0], 2)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM run_sources").fetchone()[0], 6)
        self.assertEqual(conn.execute("SELECT COUNT(DISTINCT last_changed_run_id) FROM observations").fetchone()[0], 1)

    def test_invalid_rows_and_duplicates_are_quarantined(self):
        def add_invalid(payload):
            sample = payload[1][0]
            mutations = [
                ("missing_key", lambda r: r.pop("indicator")),
                ("invalid_year", lambda r: r.update(date="dos mil")),
                ("invalid_value_type", lambda r: r.update(value=True)),
                ("null_value", lambda r: r.update(value=None)),
                ("invalid_country", lambda r: r["country"].update(id="XX")),
                ("invalid_gdp", lambda r: r.update(value=-1)),
            ]
            for _, mutate in mutations:
                row = copy.deepcopy(sample)
                mutate(row)
                payload[1].append(row)
            payload[1].append(copy.deepcopy(sample))
            payload[0]["total"] = len(payload[1])
        self.change_payload("gdp_current_usd.json", add_invalid)
        result = pipeline.run_pipeline(self.raw, self.output)
        self.assertEqual(result["metrics"]["input_rows"], 43)
        self.assertEqual(result["metrics"]["accepted_rows"], 36)
        self.assertEqual(result["metrics"]["rejected_rows"], 7)
        self.assertEqual(result["status"], "completed_with_warnings")
        self.assertEqual(set(result["rejection_reasons"]), {
            "missing_key", "invalid_year", "invalid_value_type", "null_value",
            "invalid_country", "invalid_gdp", "duplicate_key",
        })
        conn = self.connect()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM quality_rejections").fetchone()[0], 7)
        with (self.output / result["rejections"]).open(encoding="utf-8", newline="") as file:
            self.assertEqual(len(list(csv.DictReader(file))), 7)

    def test_upsert_updates_only_changed_observation(self):
        pipeline.run_pipeline(self.raw, self.output)
        original = json.loads((self.raw / "gdp_current_usd.json").read_text())[1][0]["value"]
        self.change_payload("gdp_current_usd.json", lambda p: p[1][0].update(value=original + 100))
        result = pipeline.run_pipeline(self.raw, self.output)
        self.assertEqual(result["metrics"]["updated_rows"], 1)
        self.assertEqual(result["metrics"]["unchanged_rows"], 35)
        self.assertEqual(result["metrics"]["inserted_rows"], 0)
        conn = self.connect()
        value, first, last = conn.execute(
            """SELECT value,first_loaded_run_id,last_changed_run_id FROM observations
               WHERE country_code='CO' AND year=2024 AND indicator_code='NY.GDP.MKTP.CD'"""
        ).fetchone()
        self.assertEqual(value, original + 100)
        self.assertEqual((first, last), (1, 2))
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 36)

    def test_report_and_quality_counts_reconcile_with_database(self):
        result = pipeline.run_pipeline(self.raw, self.output)
        metrics = result["metrics"]
        self.assertEqual(metrics["input_rows"], metrics["accepted_rows"] + metrics["rejected_rows"])
        self.assertEqual(metrics["accepted_rows"], metrics["inserted_rows"] + metrics["updated_rows"] + metrics["unchanged_rows"])
        self.assertEqual(metrics["missing_expected_keys"], 0)
        with (self.output / result["report"]).open(encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(len(rows), 12)
        self.assertEqual({r["pais_codigo"] for r in rows}, {"EC", "CO"})
        self.assertEqual({int(r["anio"]) for r in rows}, set(range(2019, 2025)))
        conn = self.connect()
        for row in rows:
            self.assertEqual(row["indicadores_disponibles"], "3")
            values = dict(conn.execute(
                "SELECT indicator_code,value FROM observations WHERE country_code=? AND year=?",
                (row["pais_codigo"], int(row["anio"])),
            ))
            self.assertEqual(float(row["pib_usd"]), values["NY.GDP.MKTP.CD"])
            self.assertEqual(int(row["poblacion"]), int(values["SP.POP.TOTL"]))
            self.assertEqual(float(row["inflacion_pct"]), values["FP.CPI.TOTL.ZG"])
            self.assertEqual(float(row["pib_per_capita_usd"]), round(values["NY.GDP.MKTP.CD"] / values["SP.POP.TOTL"], 2))
        stored = dict(conn.execute("SELECT metric,value FROM quality_metrics WHERE run_id=1"))
        self.assertEqual(stored, metrics)

    def test_incomplete_snapshot_fails_and_is_logged(self):
        self.change_payload("population_total.json", lambda p: p[0].update(pages=2))
        with self.assertRaisesRegex(ValueError, "respuesta incompleta"):
            pipeline.run_pipeline(self.raw, self.output)
        conn = self.connect()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 0)
        status, error = conn.execute("SELECT status,error FROM pipeline_runs").fetchone()
        self.assertEqual(status, "failed")
        self.assertIn("respuesta incompleta", error)

    def test_missing_observation_is_reported_without_imputation(self):
        def remove_one(payload):
            payload[1].pop(0)
            payload[0]["total"] -= 1
        self.change_payload("inflation_annual_pct.json", remove_one)
        result = pipeline.run_pipeline(self.raw, self.output)
        self.assertEqual(result["metrics"]["warehouse_rows"], 35)
        self.assertEqual(result["metrics"]["missing_expected_keys"], 1)
        self.assertEqual(result["status"], "completed_with_warnings")
        with (self.output / result["report"]).open(encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file))
        self.assertEqual(sum(r["indicadores_disponibles"] == "2" for r in rows), 1)
        self.assertEqual(sum(r["inflacion_pct"] == "" for r in rows), 1)

    def test_population_type_and_negative_inflation_rules(self):
        snapshots = pipeline.extract_sources(self.raw)
        population = copy.deepcopy(next(s for s in snapshots if s["indicator"] == "SP.POP.TOTL")["rows"][0])
        for invalid in (0, -2, 12.5, "1000", float("nan"), float("inf")):
            population["value"] = invalid
            with self.assertRaises(pipeline.RowValidationError):
                pipeline.normalize_record(population, "SP.POP.TOTL")
        inflation = copy.deepcopy(next(s for s in snapshots if s["indicator"] == "FP.CPI.TOTL.ZG")["rows"][0])
        inflation["value"] = -1.5
        self.assertEqual(pipeline.normalize_record(inflation, "FP.CPI.TOTL.ZG")["value"], -1.5)


if __name__ == "__main__":
    unittest.main()
