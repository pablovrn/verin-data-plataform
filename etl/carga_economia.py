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
ETL para la tabla fact_empresas_sector en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""
try:
    url = "https://www.ige.gal/igebdt/igeapi/csv/datos/7993/0:1:19:172:186,1:1:2:3:4:5,9931:32021:32028:32039:32050:32053:32071:32085:32091"

    # Leer CSV directamente desde la web
    df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

    # Sacar la dimension sector de la columna CNAE 2009 quitandole el numero de la clase
    df['sector'] = df['CNAE 2009'].str.replace(r'^\d+\s*', '', regex=True)

    # Insertar nuevos sectores en la dimensión sector si no existen
    sectores_dw = pd.read_sql("SELECT nombre FROM verin_dw.dim_sector_economico", engine)
    for sector in df['sector'].unique():
        if sector not in sectores_dw['nombre'].values:
            with engine.connect() as connection:
                connection.execute(text("INSERT INTO verin_dw.dim_sector_economico (nombre) VALUES (:nombre)"), {"nombre": sector})
                connection.commit()

    # Sacar la dimesión tipo de empresa de la columna Personalidade xurídica
    df['tipo_empresa'] = df['Personalidade xurídica']

    # Insertar nuevos tipos de empresa en la dimensión tipo_empresa si no existen
    tipos_dw = pd.read_sql("SELECT nombre FROM verin_dw.dim_tipo_empresa", engine)
    for tipo in df['tipo_empresa'].unique():
        if tipo not in tipos_dw['nombre'].values:
            with engine.connect() as connection:
                connection.execute(text("INSERT INTO verin_dw.dim_tipo_empresa (nombre) VALUES (:nombre)"), {"nombre": tipo})
                connection.commit()
                
    # Borrar columnas innecesarias
    df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT', 'CNAE 2009', 'Personalidade xurídica'])

    # Hacer los mapings de las dimensiones sector y tipo_empresa
    sectores_dw = pd.read_sql("SELECT id_sector_economico, nombre FROM verin_dw.dim_sector_economico", engine)
    tipos_dw = pd.read_sql("SELECT id_tipo_empresa, nombre FROM verin_dw.dim_tipo_empresa", engine)
    df = df.merge(sectores_dw, left_on='sector', right_on='nombre', how='left').drop(columns=['sector', 'nombre'])
    df = df.merge(tipos_dw, left_on='tipo_empresa', right_on='nombre', how='left').drop(columns=['tipo_empresa', 'nombre'])

    # Renombrar columnas para que coincidan con la tabla fact_empresas_sector
    df = df.rename(columns={
        'DatoN': 'empresas_total',
        'CodEspazo': 'id_municipio',
        'Tempo': 'id_fecha'})

    # Convertir id_fecha a formato date
    df['id_fecha'] = pd.to_datetime(df['id_fecha'], format='%Y').dt.date

    # Truncar la tabla fact_empresas_sector antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.fact_empresas_sector"))
        connection.commit()

    # Insertar datos en la tabla fact_empresas_sector
    df.to_sql('fact_empresas_sector', engine, schema='verin_dw', if_exists='append', index=False)

    print("ETL de fact_empresas_sector completado con éxito.")
except Exception as e:
    print(f"Error en el ETL de fact_empresas_sector: {e}")
    exit(1)

"""
ETL para la tabla fact_empresas_asalariados en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

try:
    url = "https://www.ige.gal/igebdt/igeapi/csv/datos/7996/0:1:2:3:4:5:6:7:8:9:10,9931:32021:32028:32039:32050:32053:32071:32085:32091"

    # Leer CSV directamente desde la web
    df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

    # Sacar la dimension rango_asalariados de la columna "Estrato de asalariados" quitandole el inicio "de "
    df['rango_asalariados'] = df['Estrato de asalariados'].str.replace(r'^de\s*', '', regex=True)

    # Insertar nuevos rangos de asalariados en la dimensión rango_asalariados si no existen
    rangos_dw = pd.read_sql("SELECT rango FROM verin_dw.dim_rango_asalariados", engine)
    for rango in df['rango_asalariados'].unique():
        if rango not in rangos_dw['rango'].values:
            with engine.connect() as connection:
                connection.execute(text("INSERT INTO verin_dw.dim_rango_asalariados (rango) VALUES (:rango)"), {"rango": rango})
                connection.commit()

    # Borrar columnas innecesarias
    df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT', 'Estrato de asalariados'])

    # Hacer los mapings de las dimensiones sector y tipo_empresa
    rangos_dw = pd.read_sql("SELECT id_rango_asalariados, rango FROM verin_dw.dim_rango_asalariados", engine)
    df = df.merge(rangos_dw, left_on='rango_asalariados', right_on='rango', how='left').drop(columns=['rango_asalariados', 'rango'])

    # Renombrar columnas para que coincidan con la tabla fact_empresas_asalariados
    df = df.rename(columns={
        'DatoN': 'empresas_total',
        'CodEspazo': 'id_municipio',
        'Tempo': 'id_fecha'})

    # Convertir id_fecha a formato date
    df['id_fecha'] = pd.to_datetime(df['id_fecha'], format='%Y').dt.date

    # Truncar la tabla fact_empresas_asalariados antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.fact_empresas_asalariados"))
        connection.commit()

    # Insertar datos en la tabla fact_empresas_asalariados
    df.to_sql('fact_empresas_asalariados', engine, schema='verin_dw', if_exists='append', index=False)

    print("ETL de fact_empresas_asalariados completado con éxito.")
except Exception as e:
    print(f"Error en el ETL de fact_empresas_asalariados: {e}")
    exit(1)

"""
ETL para la tabla fact_macros_economicos en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""    
try:
    url_renta = "https://www.ige.gal/igebdt/igeapi/csv/datos/9642/9915:32021:32028:32039:32050:32053:32071:32085:32091"
    url_pib = "https://www.ige.gal/igebdt/igeapi/csv/datos/9958/1:2,9915:32021:32028:32039:32050:32053:32071:32085:32091"

    # Leer CSV directamente desde la web
    df_renta = pd.read_csv(url_renta, sep=',', encoding='latin1', header=0)
    df_pib = pd.read_csv(url_pib, sep=',', encoding='latin1', header=0)

    # Borrar columnas innecesarias
    df_renta = df_renta.drop(columns=['CodTempo', 'Espazo', 'DatoT', 'Variables'])
    df_pib = df_pib.drop(columns=['CodTempo', 'Espazo', 'DatoT', 'Unidade', 'Variables'])

    # Renombrar columnas para que coincidan con la tabla fact_macros_economicos
    df_renta = df_renta.rename(columns={
        'DatoN': 'renta_bruta_per_capita',
        'CodEspazo': 'id_municipio',
        'Tempo': 'id_fecha'})
    df_pib = df_pib.rename(columns={
        'DatoN': 'pib_per_capita',
        'CodEspazo': 'id_municipio',
        'Tempo': 'id_fecha'})
    
    # Juntar ambos dataframes por id_municipio e id_fecha
    df = df_renta.merge(df_pib, on=['id_municipio', 'id_fecha'], how='outer')

    # Convertir id_fecha a formato date
    df['id_fecha'] = pd.to_datetime(df['id_fecha'], format='%Y').dt.date

    # Truncar la tabla fact_macros_economicos antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.fact_macros_economicos"))
        connection.commit()

    # Insertar datos en la tabla fact_macros_economicos
    df.to_sql('fact_macros_economicos', engine, schema='verin_dw', if_exists='append', index=False)

    print("ETL de fact_macros_economicos completado con éxito.")
except Exception as e:
    print(f"Error en el ETL de fact_macros_economicos: {e}")
    exit(1)