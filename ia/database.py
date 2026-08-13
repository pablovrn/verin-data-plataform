import os
import re
import psycopg2
from dotenv import load_dotenv

load_dotenv()


DIMENSION_TABLES = [
    "dim_municipio",
    "dim_tipo_educacion",
    "dim_tipo_sanitario",
    "dim_sector_economico",
    "dim_tipo_empresa",
    "dim_tipo_negocio",
    "dim_rango_asalariados",
    "dim_grupo_edad",
    "dim_lugar_nacimiento",
]



def get_connection():

    USER = os.getenv("user")
    PASSWORD = os.getenv("password")
    HOST = os.getenv("host")
    PORT = os.getenv("port")
    DBNAME = os.getenv("dbname")

    return psycopg2.connect(
        host=HOST,
        port=PORT,
        dbname=DBNAME,
        user=USER,
        password=PASSWORD,
    )

def get_schema():
    connection = get_connection()
    cursor = connection.cursor()

    # =========================================================
    # TABLAS Y COLUMNAS
    # =========================================================

    columns_query = """
        SELECT
            table_name,
            column_name,
            data_type,
            is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'verin_dw'
        ORDER BY table_name, ordinal_position;
    """

    cursor.execute(columns_query)
    columns = cursor.fetchall()

    # =========================================================
    # PRIMARY KEYS
    # =========================================================

    pk_query = """
        SELECT
            tc.table_name,
            kcu.column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = 'verin_dw'
        ORDER BY tc.table_name, kcu.ordinal_position;
    """

    cursor.execute(pk_query)
    primary_keys = cursor.fetchall()

    # =========================================================
    # FOREIGN KEYS
    # =========================================================

    fk_query = """
        SELECT
            tc.table_name,
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = 'verin_dw'
        ORDER BY tc.table_name, kcu.ordinal_position;
    """

    cursor.execute(fk_query)
    foreign_keys = cursor.fetchall()

    cursor.close()
    connection.close()

    schema = {}

    # =========================================================
    # CREAR ESTRUCTURA DEL ESQUEMA
    # =========================================================

    for table_name, column_name, data_type, is_nullable in columns:

        if table_name not in schema:
            schema[table_name] = {
                "columns": [],
                "primary_keys": [],
                "foreign_keys": [],
                "values": {},
                "last_date": None,
            }

        schema[table_name]["columns"].append({
            "name": column_name,
            "type": data_type,
            "nullable": is_nullable == "YES",
        })

    # =========================================================
    # PRIMARY KEYS
    # =========================================================

    for table_name, column_name in primary_keys:

        if table_name in schema:
            schema[table_name]["primary_keys"].append(
                column_name
            )

    # =========================================================
    # FOREIGN KEYS
    # =========================================================

    for (
        table_name,
        column_name,
        foreign_table_name,
        foreign_column_name,
    ) in foreign_keys:

        if table_name in schema:
            schema[table_name]["foreign_keys"].append({
                "column": column_name,
                "references_table": foreign_table_name,
                "references_column": foreign_column_name,
            })

    # =========================================================
    # VALORES REALES DE LAS DIMENSIONES
    # =========================================================

    for table_name in DIMENSION_TABLES:

        if table_name not in schema:
            continue

        text_columns = [
            column["name"]
            for column in schema[table_name]["columns"]
            if column["type"] in (
                "character varying",
                "character",
                "text",
            )
        ]

        for column_name in text_columns:

            values = get_dimension_values(
                table_name,
                column_name,
            )

            schema[table_name]["values"][column_name] = values

    # =========================================================
    # ÚLTIMA FECHA DE LAS TABLAS DE HECHOS
    # =========================================================

    for table_name in schema:

        if table_name.startswith("fact_"):

            schema[table_name]["last_date"] = get_last_date(
                table_name
            )

    return schema


def get_dimension_values(table_name, column_name):

    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        SELECT DISTINCT "{column_name}"
        FROM verin_dw."{table_name}"
        WHERE "{column_name}" IS NOT NULL
        ORDER BY "{column_name}";
    """

    cursor.execute(query)

    values = [
        row[0]
        for row in cursor.fetchall()
    ]

    cursor.close()
    connection.close()

    return values


def get_last_date(table_name):

    """
    Devuelve la última fecha disponible de una tabla de hechos.

    Se utiliza únicamente para proporcionar contexto al LLM.
    La consulta generada deberá utilizar MAX(id_fecha)
    cuando la pregunta sea puntual y no especifique fecha.
    """

    connection = get_connection()
    cursor = connection.cursor()

    query = f"""
        SELECT MAX(id_fecha)
        FROM verin_dw."{table_name}";
    """

    try:
        cursor.execute(query)
        result = cursor.fetchone()

        if result:
            return result[0]

        return None

    except Exception:
        return None

    finally:
        cursor.close()
        connection.close()


def schema_to_text(schema):

    schema_text = ""

    for table_name, table_info in schema.items():

        schema_text += f"\nTABLE verin_dw.{table_name}\n"

        # =====================================================
        # COLUMNAS
        # =====================================================

        schema_text += "COLUMNS:\n"

        for column in table_info["columns"]:

            nullable = (
                "NULL"
                if column["nullable"]
                else "NOT NULL"
            )

            schema_text += (
                f"- {column['name']} "
                f"{column['type']} "
                f"{nullable}\n"
            )

        # =====================================================
        # PRIMARY KEY
        # =====================================================

        if table_info["primary_keys"]:

            schema_text += "PRIMARY KEY:\n"

            for column in table_info["primary_keys"]:
                schema_text += f"- {column}\n"

        # =====================================================
        # FOREIGN KEYS
        # =====================================================

        if table_info["foreign_keys"]:

            schema_text += "FOREIGN KEYS:\n"

            for fk in table_info["foreign_keys"]:

                schema_text += (
                    f"- {fk['column']} -> "
                    f"verin_dw.{fk['references_table']}."
                    f"{fk['references_column']}\n"
                )

        # =====================================================
        # VALORES DE LAS DIMENSIONES
        # =====================================================

        if table_info["values"]:

            schema_text += "AVAILABLE VALUES:\n"

            for column_name, values in table_info["values"].items():

                if not values:
                    continue

                schema_text += f"- {column_name}:\n"

                for value in values:
                    schema_text += f"  - {value}\n"

        # =====================================================
        # ÚLTIMA FECHA
        # =====================================================

        if table_info["last_date"] is not None:

            schema_text += (
                "LATEST AVAILABLE DATE:\n"
                f"- {table_info['last_date']}\n"
            )

        schema_text += "\n"

    return schema_text


# =============================================================
# VALIDACIÓN SQL
# =============================================================

def validate_sql(sql: str):

    if not sql:
        return False, "La consulta SQL está vacía."

    sql = sql.strip()

    sql_without_final_semicolon = sql.rstrip(";").strip()

    # No múltiples sentencias.
    if ";" in sql_without_final_semicolon:
        return False, "No se permiten múltiples sentencias SQL."

    # Solo SELECT o WITH.
    if not sql_without_final_semicolon.upper().startswith(
        ("SELECT", "WITH")
    ):
        return False, (
            "Solo se permiten consultas SELECT o WITH."
        )

    forbidden_keywords = [
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "CREATE",
        "TRUNCATE",
        "GRANT",
        "REVOKE",
        "MERGE",
        "CALL",
        "EXEC",
        "EXECUTE",
        "COPY",
        "VACUUM",
        "ANALYZE",
        "COMMENT",
    ]

    for keyword in forbidden_keywords:

        if f" {keyword} " in (
            f" {sql_without_final_semicolon.upper()} "
        ):
            return False, (
                f"La consulta contiene una operación "
                f"no permitida: {keyword}"
            )

    forbidden_schemas = [
        "pg_catalog",
        "information_schema",
        "pg_toast",
    ]

    for schema_name in forbidden_schemas:

        if f"{schema_name}." in sql_without_final_semicolon.lower():
            return False, (
                f"No se permite acceder al esquema "
                f"{schema_name}."
            )

    # Comprobar referencias directas a tablas.
    import re

    table_references = re.findall(
        r"\b(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_\.]*)",
        sql_without_final_semicolon,
        re.IGNORECASE,
    )

    for table in table_references:

        if "." not in table:
            return False, (
                f"La tabla '{table}' no utiliza "
                f"el esquema verin_dw."
            )

        schema_name = table.split(".")[0].strip('"')

        if schema_name.lower() != "verin_dw":
            return False, (
                f"No se permite consultar el esquema "
                f"'{schema_name}'."
            )

    return True, None


# =============================================================
# EJECUCIÓN
# =============================================================

def execute_sql(sql: str):

    is_valid, error = validate_sql(sql)

    if not is_valid:
        raise ValueError(error)

    sql = sql.rstrip(";").strip()

    connection = get_connection()

    try:

        cursor = connection.cursor()

        # Transacción de solo lectura.
        cursor.execute(
            "SET TRANSACTION READ ONLY"
        )

        # Máximo 10 segundos de ejecución.
        cursor.execute(
            "SET LOCAL statement_timeout = '10000'"
        )

        cursor.execute(sql)

        columns = [
            description[0]
            for description in cursor.description
        ]

        rows = cursor.fetchall()

        results = [
            dict(zip(columns, row))
            for row in rows
        ]

        cursor.close()

        connection.rollback()

        return results

    except Exception:

        connection.rollback()
        raise

    finally:

        connection.close()