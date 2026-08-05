import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from sqlalchemy import text

load_dotenv(override=True)

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Construct the SQLAlchemy connection string
DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}?sslmode=require"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

try:
    with engine.connect() as connection:
        print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")
    exit(1)

"""
ETL para la tabla fact_personal_sanitario en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""
try:
    url = "https://www.ige.gal/igebdt/igeapi/csv/datos/115/9915:32021:32028:32039:32050:32053:32071:32085:32091"

    # Leer CSV directamente desde la web
    df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

    # Borrar columnas innecesarias
    df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT'])

    # Renombrar columnas para que coincidan con la tabla fact_personal_sanitario
    df.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'DatoN': 'total'}, inplace=True)

    # Añadir los tipos de personal sanitario a la dimensión dim_tipo_sanitario si no existen
    tipos_dw = pd.read_sql("SELECT nombre FROM verin_dw.dim_tipo_sanitario", engine)
    for tipo in df['Recursos humanos'].unique():
        if tipo not in tipos_dw['nombre'].values:
            with engine.connect() as connection:
                connection.execute(text("INSERT INTO verin_dw.dim_tipo_sanitario (nombre) VALUES (:tipo)"), {"tipo": tipo})
                connection.commit()

    # Mapear los tipos de personal sanitario a sus IDs correspondientes en la dimensión dim_tipo_sanitario
    tipos_mapping = pd.read_sql("SELECT id_tipo_sanitario, nombre FROM verin_dw.dim_tipo_sanitario", engine)
    df = df.merge(tipos_mapping, how='left', left_on='Recursos humanos', right_on='nombre')
    df.drop(columns=['Recursos humanos', 'nombre'], inplace=True)

    # Convertir id_fecha a formato DATE
    df['id_fecha'] = pd.to_datetime(df['id_fecha'], format='%Y').dt.date

    # Truncar la tabla fact_personal_sanitario antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.fact_personal_sanitario"))
        connection.commit()

    # Insertar datos en la tabla fact_personal_sanitario
    df.to_sql('fact_personal_sanitario', engine, schema='verin_dw', if_exists='append', index=False)
    print("ETL de fact_personal_sanitario completado con éxito.")

except Exception as e:
    print(f"Error en el ETL de fact_personal_sanitario: {e}")
    exit(1)

"""
ETL para la tabla fact_alumnos_tipo_educacion en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""
try:
    url = "https://www.ige.gal/igebdt/igeapi/csv/datos/8057/1:0,9915:32021:32028:32039:32050:32053:32071:32085:32091"
    # Leer CSV directamente desde la web
    df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

    # Borrar columnas innecesarias
    df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT', 'Sexo'])

    # Renombrar columnas para que coincidan con la tabla fact_alumnos_tipo_educacion
    df.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'DatoN': 'total'}, inplace=True)

    # Añadir los tipos de educación a la dimensión dim_tipo_educacion si no existen
    tipos_dw = pd.read_sql("SELECT nombre FROM verin_dw.dim_tipo_educacion", engine)
    for tipo in df['Nivel de ensinanza'].unique():
        if tipo not in tipos_dw['nombre'].values:
            with engine.connect() as connection:
                connection.execute(text("INSERT INTO verin_dw.dim_tipo_educacion (nombre) VALUES (:tipo)"), {"tipo": tipo})
                connection.commit()

    # Mapear los tipos de educación a sus IDs correspondientes en la dimensión dim_tipo_educacion
    tipos_mapping = pd.read_sql("SELECT id_tipo_educacion, nombre FROM verin_dw.dim_tipo_educacion", engine)
    df = df.merge(tipos_mapping, how='left', left_on='Nivel de ensinanza', right_on='nombre')
    df.drop(columns=['Nivel de ensinanza', 'nombre'], inplace=True)

    # Convertir id_fecha a formato DATE
    df['id_fecha'] = pd.to_datetime(df['id_fecha'], format='%Y').dt.date

    # Truncar la tabla fact_alumnos_tipo_educacion antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.fact_alumnos_tipo_educacion"))
        connection.commit()

    # Insertar datos en la tabla fact_alumnos_tipo_educacion
    df.to_sql('fact_alumnos_tipo_educacion', engine, schema='verin_dw', if_exists='append', index=False)
    print("ETL de fact_alumnos_tipo_educacion completado con éxito.")

except Exception as e:
    print(f"Error en el ETL de fact_alumnos_tipo_educacion: {e}")
    exit(1)