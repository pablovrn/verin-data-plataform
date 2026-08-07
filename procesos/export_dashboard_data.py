import json
import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
OUTPUT_PATH = ROOT_DIR / "docs" / "data" / "dashboard.json"

load_dotenv(BASE_DIR / ".env", override=True)
load_dotenv(ROOT_DIR / ".env", override=True)

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL and all([USER, PASSWORD, HOST, PORT, DBNAME]):
    DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"


def json_default(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def normalize_text(value):
    replacements = str(value or "").lower()
    for source, target in (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
        ("ü", "u"),
        ("ñ", "n"),
    ):
        replacements = replacements.replace(source, target)
    return replacements


def rows_to_dicts(rows):
    return [dict(row) for row in rows]


def get_connection():
    if not DATABASE_URL:
        raise RuntimeError("No se encontro configuracion de base de datos en .env.")
    return psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


def fetch_all(query, params=None):
    with get_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return rows_to_dicts(cursor.fetchall())


def fetch_municipios():
    return fetch_all(
        """
        SELECT id_municipio, nombre
        FROM verin_dw.dim_municipio
        ORDER BY nombre ASC
        """
    )


def fetch_population(municipio_id):
    return fetch_all(
        """
        SELECT id_fecha, poblacion_total, hombres, mujeres
        FROM verin_dw.fact_poblacion
        WHERE id_municipio = %s
        ORDER BY id_fecha ASC
        """,
        (municipio_id,),
    )


def fetch_births(municipio_id):
    return fetch_all(
        """
        SELECT id_fecha, nacimientos_total
        FROM verin_dw.fact_nacimientos
        WHERE id_municipio = %s
        ORDER BY id_fecha ASC
        """,
        (municipio_id,),
    )


def fetch_deaths(municipio_id):
    return fetch_all(
        """
        SELECT id_fecha, defunciones_total
        FROM verin_dw.fact_defunciones
        WHERE id_municipio = %s
        ORDER BY id_fecha ASC
        """,
        (municipio_id,),
    )


def fetch_age_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.id_grupo_edad,
            f.poblacion_total,
            f.hombres,
            f.mujeres,
            d.rango
        FROM verin_dw.fact_poblacion_edad f
        JOIN verin_dw.dim_grupo_edad d ON d.id_grupo_edad = f.id_grupo_edad
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, f.id_grupo_edad ASC
        """,
        (municipio_id,),
    )


def fetch_origin_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.id_lugar_nacimiento,
            f.poblacion_total,
            d.nombre
        FROM verin_dw.fact_poblacion_lugar f
        JOIN verin_dw.dim_lugar_nacimiento d ON d.id_lugar_nacimiento = f.id_lugar_nacimiento
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, f.poblacion_total DESC, d.nombre ASC
        """,
        (municipio_id,),
    )


def fetch_economic_sector_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.empresas_total,
            s.nombre AS sector,
            t.nombre AS tipo_empresa
        FROM verin_dw.fact_empresas_sector f
        JOIN verin_dw.dim_sector_economico s ON s.id_sector_economico = f.id_sector_economico
        JOIN verin_dw.dim_tipo_empresa t ON t.id_tipo_empresa = f.id_tipo_empresa
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, s.nombre ASC, t.nombre ASC
        """,
        (municipio_id,),
    )


def fetch_economic_employee_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.empresas_total,
            r.rango
        FROM verin_dw.fact_empresas_asalariados f
        JOIN verin_dw.dim_rango_asalariados r ON r.id_rango_asalariados = f.id_rango_asalariados
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, f.id_rango_asalariados ASC
        """,
        (municipio_id,),
    )


def fetch_economic_macro_rows(municipio_id):
    return fetch_all(
        """
        SELECT id_fecha, renta_bruta_per_capita, pib_per_capita
        FROM verin_dw.fact_macros_economicos
        WHERE id_municipio = %s
        ORDER BY id_fecha ASC
        """,
        (municipio_id,),
    )


def fetch_healthcare_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.total,
            d.nombre AS tipo_sanitario
        FROM verin_dw.fact_personal_sanitario f
        JOIN verin_dw.dim_tipo_sanitario d ON d.id_tipo_sanitario = f.id_tipo_sanitario
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, d.nombre ASC
        """,
        (municipio_id,),
    )


def fetch_education_rows(municipio_id):
    return fetch_all(
        """
        SELECT
            f.id_fecha,
            f.total,
            d.nombre AS tipo_educacion
        FROM verin_dw.fact_alumnos_tipo_educacion f
        JOIN verin_dw.dim_tipo_educacion d ON d.id_tipo_educacion = f.id_tipo_educacion
        WHERE f.id_municipio = %s
        ORDER BY f.id_fecha ASC, d.nombre ASC
        """,
        (municipio_id,),
    )


def latest_rows_by_year(rows):
    grouped = {}
    latest_dates = {}

    for row in rows:
        year = row["id_fecha"].year
        row_date = row["id_fecha"]
        latest_date = latest_dates.get(year)

        if latest_date is None or row_date > latest_date:
            latest_dates[year] = row_date
            grouped[year] = [row]
        elif row_date == latest_date:
            grouped[year].append(row)

    return {str(year): rows for year, rows in grouped.items()}


def build_payload():
    municipios = fetch_municipios()
    default_row = next((row for row in municipios if normalize_text(row["nombre"]) == "verin"), municipios[0] if municipios else None)

    payload = {
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "default_municipio_id": default_row["id_municipio"] if default_row else None,
        "municipios": municipios,
        "series": {},
    }

    for municipio in municipios:
        municipio_id = str(municipio["id_municipio"])
        population = fetch_population(municipio["id_municipio"])
        births = fetch_births(municipio["id_municipio"])
        deaths = fetch_deaths(municipio["id_municipio"])
        age_rows = fetch_age_rows(municipio["id_municipio"])
        origin_rows = fetch_origin_rows(municipio["id_municipio"])
        economic_sector_rows = fetch_economic_sector_rows(municipio["id_municipio"])
        economic_employee_rows = fetch_economic_employee_rows(municipio["id_municipio"])
        economic_macro_rows = fetch_economic_macro_rows(municipio["id_municipio"])
        healthcare_rows = fetch_healthcare_rows(municipio["id_municipio"])
        education_rows = fetch_education_rows(municipio["id_municipio"])

        payload["series"][municipio_id] = {
            "population": population,
            "births": births,
            "deaths": deaths,
            "age_by_year": latest_rows_by_year(age_rows),
            "origin_by_year": latest_rows_by_year(origin_rows),
            "economy": {
                "companies_by_sector_year": latest_rows_by_year(economic_sector_rows),
                "companies_by_employee_year": latest_rows_by_year(economic_employee_rows),
                "macros": economic_macro_rows,
            },
            "services": {
                "healthcare_by_year": latest_rows_by_year(healthcare_rows),
                "education_by_year": latest_rows_by_year(education_rows),
            },
        }

    return payload


def main():
    payload = build_payload()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(payload, ensure_ascii=True, default=json_default, separators=(",", ":")),
        encoding="utf-8",
    )
    print(f"Datos exportados a {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
