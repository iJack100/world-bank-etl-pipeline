"""ETL reproducible del Banco Mundial. Python estándar; sin red por defecto."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import shutil
import sqlite3
from urllib.request import Request, urlopen
import uuid


ROOT = Path(__file__).resolve().parent
COUNTRIES = {"EC": ("Ecuador", "ECU"), "CO": ("Colombia", "COL")}
YEARS = tuple(range(2019, 2025))
INDICATORS = {
    "NY.GDP.MKTP.CD": ("gdp_current_usd.json", "PIB", "USD corrientes"),
    "SP.POP.TOTL": ("population_total.json", "Población", "personas"),
    "FP.CPI.TOTL.ZG": ("inflation_annual_pct.json", "Inflación", "% anual"),
}
EXPECTED_KEYS = {
    (country, year, indicator)
    for country in COUNTRIES
    for year in YEARS
    for indicator in INDICATORS
}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def source_url(indicator):
    return (
        "https://api.worldbank.org/v2/country/EC;CO/indicator/"
        f"{indicator}?date=2019:2024&format=json&per_page=1000"
    )


def parse_envelope(content, label):
    """No aceptar páginas truncadas ni respuestas de error como si fueran datos."""
    payload = json.loads(content.decode("utf-8-sig"))
    if not (
        isinstance(payload, list) and len(payload) == 2
        and isinstance(payload[0], dict) and isinstance(payload[1], list)
    ):
        raise ValueError(f"{label}: respuesta JSON sin estructura [metadatos, filas]")
    metadata, rows = payload
    if metadata.get("page") != 1 or metadata.get("pages") != 1:
        raise ValueError(f"{label}: respuesta incompleta; se requiere una sola página")
    if metadata.get("total") != len(rows):
        raise ValueError(f"{label}: el total declarado no coincide con las filas")
    return metadata, rows


def refresh_sources(raw_dir):
    """Actualizar snapshots solo después de descargar las tres respuestas completas."""
    raw_dir = Path(raw_dir).resolve()
    raw_dir.parent.mkdir(parents=True, exist_ok=True)
    manifest = []
    stage = raw_dir.parent / f"_download_{uuid.uuid4().hex}"
    stage.mkdir()
    try:
        for indicator, (filename, _, _) in INDICATORS.items():
            url = source_url(indicator)
            request = Request(url, headers={"User-Agent": "IndicadoresETL/1.0"})
            with urlopen(request, timeout=45) as response:
                content = response.read()
            parse_envelope(content, filename)
            (stage / filename).write_bytes(content)
            manifest.append({
                "indicator": indicator, "file": filename, "url": url,
                "retrieved_at_utc": utc_now(),
            })
        raw_dir.mkdir(parents=True, exist_ok=True)
        for entry in manifest:
            (stage / entry["file"]).replace(raw_dir / entry["file"])
        (raw_dir / "sources.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
    finally:
        if stage.resolve().parent != raw_dir.parent.resolve():
            raise RuntimeError("El staging debe permanecer dentro del directorio de datos")
        shutil.rmtree(stage)


def extract_sources(raw_dir):
    raw_dir = Path(raw_dir)
    manifest = json.loads((raw_dir / "sources.json").read_text(encoding="utf-8-sig"))
    if not isinstance(manifest, list):
        raise ValueError("sources.json debe ser una lista")
    snapshots = []
    seen = set()
    for entry in manifest:
        indicator = entry.get("indicator") if isinstance(entry, dict) else None
        if indicator not in INDICATORS or indicator in seen:
            raise ValueError("Manifest: indicador desconocido o repetido")
        seen.add(indicator)
        filename = INDICATORS[indicator][0]
        if entry.get("file") != filename or entry.get("url") != source_url(indicator):
            raise ValueError("Manifest: archivo o URL no corresponde al indicador")
        timestamp = entry.get("retrieved_at_utc")
        if not isinstance(timestamp, str):
            raise ValueError("Manifest: falta fecha de descarga UTC")
        parsed_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed_date.utcoffset() is None:
            raise ValueError("Manifest: la fecha debe incluir zona horaria")
        content = (raw_dir / filename).read_bytes()
        metadata, rows = parse_envelope(content, filename)
        snapshots.append({
            **entry, "rows": rows, "lastupdated": metadata.get("lastupdated", ""),
            "sha256": hashlib.sha256(content).hexdigest(),
        })
    if seen != set(INDICATORS):
        raise ValueError("Manifest: deben existir los tres indicadores")
    return snapshots


class RowValidationError(ValueError):
    pass


def normalize_record(row, expected_indicator):
    if not isinstance(row, dict):
        raise RowValidationError("invalid_record_type")
    if not {"indicator", "country", "countryiso3code", "date", "value"} <= row.keys():
        raise RowValidationError("missing_key")
    if not isinstance(row["country"], dict) or not isinstance(row["indicator"], dict):
        raise RowValidationError("invalid_key_type")
    country = row["country"].get("id")
    indicator = row["indicator"].get("id")
    if not isinstance(country, str) or country not in COUNTRIES:
        raise RowValidationError("invalid_country")
    if indicator != expected_indicator:
        raise RowValidationError("invalid_indicator")
    if row["countryiso3code"] != COUNTRIES[country][1]:
        raise RowValidationError("invalid_iso3")
    year = row["date"]
    if not isinstance(year, str) or len(year) != 4 or not year.isdigit():
        raise RowValidationError("invalid_year")
    year = int(year)
    if year not in YEARS:
        raise RowValidationError("year_out_of_range")
    value = row["value"]
    if value is None:
        raise RowValidationError("null_value")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RowValidationError("invalid_value_type")
    value = float(value)
    if not math.isfinite(value):
        raise RowValidationError("non_finite_value")
    if indicator == "NY.GDP.MKTP.CD" and value <= 0:
        raise RowValidationError("invalid_gdp")
    if indicator == "SP.POP.TOTL" and (value <= 0 or not value.is_integer()):
        raise RowValidationError("invalid_population")
    # La inflación negativa es válida: no se transforma ni se descarta.
    return {"country_code": country, "year": year, "indicator_code": indicator, "value": value}


def transform(snapshots):
    accepted, rejected, seen = [], [], set()
    for source in snapshots:
        for index, raw in enumerate(source["rows"], start=1):
            try:
                row = normalize_record(raw, source["indicator"])
                key = (row["country_code"], row["year"], row["indicator_code"])
                if key in seen:
                    raise RowValidationError("duplicate_key")
                seen.add(key)
                accepted.append({**row, "source_file": source["file"]})
            except RowValidationError as error:
                rejected.append({
                    "source_file": source["file"], "record_index": index,
                    "reason": str(error), "raw_record_json": json.dumps(raw, ensure_ascii=False),
                })
    return accepted, rejected, seen


SCHEMA = """
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_utc TEXT NOT NULL, finished_at_utc TEXT,
    mode TEXT NOT NULL, status TEXT NOT NULL,
    input_rows INTEGER DEFAULT 0, accepted_rows INTEGER DEFAULT 0,
    rejected_rows INTEGER DEFAULT 0, inserted_rows INTEGER DEFAULT 0,
    updated_rows INTEGER DEFAULT 0, unchanged_rows INTEGER DEFAULT 0,
    warehouse_rows INTEGER DEFAULT 0, error TEXT
);
CREATE TABLE IF NOT EXISTS countries (
    country_code TEXT PRIMARY KEY, country_name TEXT NOT NULL, iso3 TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS indicators (
    indicator_code TEXT PRIMARY KEY, indicator_name TEXT NOT NULL, unit TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
    country_code TEXT NOT NULL REFERENCES countries(country_code),
    year INTEGER NOT NULL CHECK(year BETWEEN 2019 AND 2024),
    indicator_code TEXT NOT NULL REFERENCES indicators(indicator_code),
    value REAL NOT NULL,
    source_file TEXT NOT NULL,
    first_loaded_run_id INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    last_changed_run_id INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    PRIMARY KEY(country_code, year, indicator_code)
);
CREATE TABLE IF NOT EXISTS run_sources (
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    source_file TEXT NOT NULL, url TEXT NOT NULL, retrieved_at_utc TEXT NOT NULL,
    sha256 TEXT NOT NULL, source_last_updated TEXT, input_rows INTEGER NOT NULL,
    PRIMARY KEY(run_id, source_file)
);
CREATE TABLE IF NOT EXISTS quality_metrics (
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    metric TEXT NOT NULL, value INTEGER NOT NULL,
    PRIMARY KEY(run_id, metric)
);
CREATE TABLE IF NOT EXISTS quality_rejections (
    rejection_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL REFERENCES pipeline_runs(run_id),
    source_file TEXT NOT NULL, record_index INTEGER NOT NULL,
    reason TEXT NOT NULL, raw_record_json TEXT NOT NULL
);
"""


def load(conn, run_id, sources, rows):
    for code, (name, iso3) in COUNTRIES.items():
        conn.execute("INSERT OR IGNORE INTO countries VALUES (?, ?, ?)", (code, name, iso3))
    for code, (_, name, unit) in INDICATORS.items():
        conn.execute("INSERT OR IGNORE INTO indicators VALUES (?, ?, ?)", (code, name, unit))
    for source in sources:
        conn.execute(
            "INSERT INTO run_sources VALUES (?, ?, ?, ?, ?, ?, ?)",
            (run_id, source["file"], source["url"], source["retrieved_at_utc"],
             source["sha256"], source["lastupdated"], len(source["rows"])),
        )
    counts = Counter(inserted_rows=0, updated_rows=0, unchanged_rows=0)
    for row in rows:
        key = (row["country_code"], row["year"], row["indicator_code"])
        existing = conn.execute(
            "SELECT value FROM observations WHERE country_code=? AND year=? AND indicator_code=?",
            key,
        ).fetchone()
        action = "inserted_rows" if existing is None else (
            "unchanged_rows" if existing[0] == row["value"] else "updated_rows"
        )
        counts[action] += 1
        conn.execute(
            """INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(country_code, year, indicator_code) DO UPDATE SET
                 value=excluded.value, source_file=excluded.source_file,
                 last_changed_run_id=excluded.last_changed_run_id
               WHERE observations.value <> excluded.value""",
            (*key, row["value"], row["source_file"], run_id, run_id),
        )
    return counts


def export_report(conn, destination):
    rows = conn.execute("""
        SELECT o.country_code, c.country_name, o.year,
          MAX(CASE WHEN indicator_code='NY.GDP.MKTP.CD' THEN value END) AS gdp,
          MAX(CASE WHEN indicator_code='SP.POP.TOTL' THEN value END) AS population,
          MAX(CASE WHEN indicator_code='FP.CPI.TOTL.ZG' THEN value END) AS inflation,
          COUNT(*) AS available_indicators
        FROM observations o JOIN countries c USING(country_code)
        GROUP BY o.country_code, c.country_name, o.year ORDER BY o.country_code, o.year
    """).fetchall()
    with Path(destination).open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output)
        writer.writerow([
            "pais_codigo", "pais", "anio", "pib_usd", "poblacion",
            "inflacion_pct", "pib_per_capita_usd", "indicadores_disponibles",
        ])
        for code, name, year, gdp, population, inflation, available in rows:
            per_capita = round(gdp / population, 2) if gdp is not None and population else ""
            writer.writerow([
                code, name, year, gdp if gdp is not None else "",
                int(population) if population is not None else "",
                inflation if inflation is not None else "", per_capita, available,
            ])
    return len(rows)


def run_pipeline(raw_dir=ROOT / "data" / "raw", output_dir=ROOT / "outputs", refresh=False):
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    database = output_dir / "indicadores.sqlite"
    conn = sqlite3.connect(database)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    run_id = conn.execute(
        "INSERT INTO pipeline_runs(started_at_utc,mode,status) VALUES (?,?,'running')",
        (utc_now(), "refresh" if refresh else "offline"),
    ).lastrowid
    conn.commit()
    try:
        if refresh:
            refresh_sources(raw_dir)
        sources = extract_sources(raw_dir)
        accepted, rejected, keys = transform(sources)
        reasons = Counter(row["reason"] for row in rejected)
        with conn:
            counts = load(conn, run_id, sources, accepted)
            metrics = {
                "input_rows": sum(len(s["rows"]) for s in sources),
                "accepted_rows": len(accepted), "rejected_rows": len(rejected),
                "duplicate_rows": reasons["duplicate_key"], "null_rows": reasons["null_value"],
                "invalid_rows": len(rejected) - reasons["duplicate_key"] - reasons["null_value"],
                "expected_keys": len(EXPECTED_KEYS),
                "missing_expected_keys": len(EXPECTED_KEYS - keys),
                **dict(counts),
                "warehouse_rows": conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0],
            }
            assert metrics["input_rows"] == metrics["accepted_rows"] + metrics["rejected_rows"]
            assert metrics["accepted_rows"] == sum(counts.values())
            conn.executemany(
                "INSERT INTO quality_metrics VALUES (?, ?, ?)",
                [(run_id, key, value) for key, value in metrics.items()],
            )
            conn.executemany(
                """INSERT INTO quality_rejections
                   (run_id,source_file,record_index,reason,raw_record_json) VALUES (?,?,?,?,?)""",
                [(run_id, r["source_file"], r["record_index"], r["reason"], r["raw_record_json"])
                 for r in rejected],
            )
            status = "completed_with_warnings" if rejected or metrics["missing_expected_keys"] else "completed"
            conn.execute(
                """UPDATE pipeline_runs SET finished_at_utc=?,status=?,input_rows=?,accepted_rows=?,
                   rejected_rows=?,inserted_rows=?,updated_rows=?,unchanged_rows=?,warehouse_rows=?
                   WHERE run_id=?""",
                (utc_now(), status, *(metrics[key] for key in (
                    "input_rows", "accepted_rows", "rejected_rows", "inserted_rows",
                    "updated_rows", "unchanged_rows", "warehouse_rows")), run_id),
            )
        report_rows = export_report(conn, output_dir / "indicadores_por_pais_anio.csv")
        rejection_path = output_dir / f"rechazos_run_{run_id}.csv"
        with rejection_path.open("w", encoding="utf-8", newline="") as output:
            fields = ["source_file", "record_index", "reason", "raw_record_json"]
            writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rejected)
        summary = {
            "run_id": run_id, "status": status, "mode": "refresh" if refresh else "offline",
            "finished_at_utc": utc_now(), "metrics": metrics, "rejection_reasons": dict(reasons),
            "report_rows": report_rows, "database": database.name,
            "report": "indicadores_por_pais_anio.csv", "rejections": rejection_path.name,
            "sources": [{k: v for k, v in source.items() if k != "rows"} for source in sources],
        }
        (output_dir / "quality_report.json").write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        return summary
    except Exception as error:
        conn.rollback()
        conn.execute(
            "UPDATE pipeline_runs SET finished_at_utc=?,status='failed',error=? WHERE run_id=?",
            (utc_now(), f"{type(error).__name__}: {error}", run_id),
        )
        conn.commit()
        raise
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=ROOT / "data" / "raw")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "outputs")
    parser.add_argument("--refresh", action="store_true", help="Descargar snapshots oficiales; requiere internet")
    args = parser.parse_args()
    summary = run_pipeline(args.raw_dir, args.output_dir, args.refresh)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
