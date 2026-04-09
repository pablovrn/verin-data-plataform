import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

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
ETL para la tabla fact_poblacion en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

"""
Estructura del CSV:
- Sexo: Hombres, Mujeres, Total
- CodTempo: Código temporal (año)
- Tempo: Año
- CodEspazo: Código del espacio geográfico (municipio)
- Espazo: Nombre del espacio geográfico (municipio)
- DatoN: Número de habitantes (población)
- DatoT: Dato con punto decimal
"""

# URL del CSV
url = "https://www.ige.gal/igebdt/igeapi/csv/datos/589/9915:32021:32028:32039:32050:32053:32071:32085:32091"

# Leer CSV directamente desde la web
df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

# Borrar columnas innecesarias
df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT'])

# Pivotar el DataFrame para tener una fila por combinación de Tempo y Espazo, y columnas para cada Sexo
df_pivot = df.pivot_table(index=['Tempo', 'CodEspazo'], columns='Sexo', values='DatoN').reset_index()
df_pivot.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'Homes': 'hombres', 'Mulleres': 'mujeres', 'Total': 'poblacion_total'}, inplace=True)

# Convertir id_fecha a formato DATE
df_pivot['id_fecha'] = pd.to_datetime(df_pivot['id_fecha'], format='%Y').dt.date

# Hacer un merge para insertar solo nuevos datos
existing_data = pd.read_sql('SELECT id_fecha, id_municipio FROM verin_dw.fact_poblacion', engine)
df_merged = df_pivot.merge(existing_data, on=['id_fecha', 'id_municipio'], how='left', indicator=True)
df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insertar/actualizar en la tabla fact_poblacion
df_to_insert.to_sql('fact_poblacion', engine, schema='verin_dw', if_exists='append', index=False)


"""
ETL para la tabla fact_nacimientos en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

url = "https://www.ige.gal/igebdt/igeapi/csv/datos/57/9915:32021:32028:32039:32050:32053:32071:32085:32091"

# Leer CSV directamente desde la web
df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

#Leer datos de 2000 en adelante
df = df[df['Tempo'] >= 2000]

# Borrar columnas innecesarias
df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT'])

# Si datoN es NaN, rellenar con -1 para indicar que no hay datos disponibles
df['DatoN'] = df['DatoN'].fillna(-1)

# Pivotar el DataFrame para tener una fila por combinación de Tempo y Espazo, y columnas para cada Sexo
df_pivot = df.pivot_table(index=['Tempo', 'CodEspazo'], columns='Sexo', values='DatoN').reset_index()
df_pivot.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'Homes': 'hombres', 'Mulleres': 'mujeres', 'Total': 'nacimientos_total'}, inplace=True)

# Convertir id_fecha a formato DATE
df_pivot['id_fecha'] = pd.to_datetime(df_pivot['id_fecha'], format='%Y').dt.date

# Hacer un merge para insertar solo nuevos datos
existing_data = pd.read_sql('SELECT id_fecha, id_municipio FROM verin_dw.fact_nacimientos', engine)
df_merged = df_pivot.merge(existing_data, on=['id_fecha', 'id_municipio'], how='left', indicator=True)
df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insertar/actualizar en la tabla fact_nacimientos
df_to_insert.to_sql('fact_nacimientos', engine, schema='verin_dw', if_exists='append', index=False)

