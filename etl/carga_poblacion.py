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

"""
ETL para la tabla fact_defunciones en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

url = "https://www.ige.gal/igebdt/igeapi/csv/datos/30/9915:32021:32028:32039:32050:32053:32071:32085:32091"

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
df_pivot.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'Homes': 'hombres', 'Mulleres': 'mujeres', 'Total': 'defunciones_total'}, inplace=True)

# Convertir id_fecha a formato DATE
df_pivot['id_fecha'] = pd.to_datetime(df_pivot['id_fecha'], format='%Y').dt.date

# Hacer un merge para insertar solo nuevos datos
existing_data = pd.read_sql('SELECT id_fecha, id_municipio FROM verin_dw.fact_defunciones', engine)
df_merged = df_pivot.merge(existing_data, on=['id_fecha', 'id_municipio'], how='left', indicator=True)
df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insertar/actualizar en la tabla fact_defunciones
df_to_insert.to_sql('fact_defunciones', engine, schema='verin_dw', if_exists='append', index=False)

"""
ETL para la tabla fact_poblacion_edad en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

url = "https://www.ige.gal/igebdt/igeapi/csv/datos/142/2:1:2:3:4:5:6:7:8:9:10:11:12:13:14:15:16:17:18:22,9915:32021:32028:32039:32050:32053:32071:32085:32091"

# Leer CSV directamente desde la web
df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

#Leer datos de 2000 en adelante
df = df[df['Tempo'] >= 2000]

# Borrar columnas innecesarias
df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT'])

# Si datoN es NaN, rellenar con -1 para indicar que no hay datos disponibles
df['DatoN'] = df['DatoN'].fillna(-1)

#Recuperar los grupos de edad actuales del DW
edades_dw = pd.read_sql('SELECT * FROM verin_dw.dim_grupo_edad', engine)

# Leer la columna "Grupos de idade" para rellenar la dimensión de edad con los grupos de edad nuevos
grupos_edad = df['Grupos de idade'].unique()
for grupo in grupos_edad:
    if grupo not in edades_dw['rango'].values:
        with engine.connect() as connection:
            connection.execute(text("INSERT INTO verin_dw.dim_grupo_edad (rango) VALUES (:grupo)"), {"grupo": grupo})
            connection.commit()

#Recuperar los grupos de edad con sus IDs actualizados
edades_dw = pd.read_sql('SELECT * FROM verin_dw.dim_grupo_edad', engine)

#Mapear el grupo de edad con su ID correspondiente
df = df.merge(edades_dw, left_on='Grupos de idade', right_on='rango', how='left')

#Eliminar columnas innecesarias
df = df.drop(columns=['Grupos de idade', 'rango'])

# Pivotar el DataFrame para tener una fila por combinación de Tempo, Espazo y grupo de edad, y columnas para cada Sexo
df_pivot = df.pivot_table(index=['Tempo', 'CodEspazo', 'id_grupo_edad'], columns='Sexo', values='DatoN').reset_index()
df_pivot.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'Homes': 'hombres', 'Mulleres': 'mujeres', 'Total': 'poblacion_total'}, inplace=True)

# Convertir id_fecha a formato DATE
df_pivot['id_fecha'] = pd.to_datetime(df_pivot['id_fecha'], format='%Y').dt.date

# Hacer un merge para insertar solo nuevos datos
existing_data = pd.read_sql('SELECT id_fecha, id_municipio FROM verin_dw.fact_poblacion_edad', engine)
df_merged = df_pivot.merge(existing_data, on=['id_fecha', 'id_municipio'], how='left', indicator=True)
df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insertar/actualizar en la tabla fact_poblacion_edad
df_to_insert.to_sql('fact_poblacion_edad', engine, schema='verin_dw', if_exists='append', index=False)

"""
ETL para la tabla fact_poblacion_lugar en la base de datos verin_dw.
Fuente de datos: Instituto Galego de Estatística (IGE)
"""

url = "https://www.ige.gal/igebdt/igeapi/csv/datos/143/2:1:2:3:4:5,9915:32021:32028:32039:32050:32053:32071:32085:32091"

# Leer CSV directamente desde la web
df = pd.read_csv(url, sep=',', encoding='latin1', header=0)

#Leer datos de 2000 en adelante
df = df[df['Tempo'] >= 2000]

# Borrar columnas innecesarias
df = df.drop(columns=['CodTempo', 'Espazo', 'DatoT'])

# Si datoN es NaN, rellenar con -1 para indicar que no hay datos disponibles
df['DatoN'] = df['DatoN'].fillna(-1)

#Recuperar los grupos de edad actuales del DW
edades_dw = pd.read_sql('SELECT * FROM verin_dw.dim_lugar_nacimiento', engine)

# Leer la columna "Lugar de nacemento" para rellenar la dimensión de lugar de nacimiento con los lugares de nacimiento nuevos
grupos_edad = df['Lugar de nacemento'].unique()
for grupo in grupos_edad:
    if grupo not in edades_dw['nombre'].values:
        with engine.connect() as connection:
            connection.execute(text("INSERT INTO verin_dw.dim_lugar_nacimiento (nombre) VALUES (:grupo)"), {"grupo": grupo})
            connection.commit()

#Recuperar los grupos de edad con sus IDs actualizados
edades_dw = pd.read_sql('SELECT * FROM verin_dw.dim_lugar_nacimiento', engine)

#Mapear el grupo de edad con su ID correspondiente
df = df.merge(edades_dw, left_on='Lugar de nacemento', right_on='nombre', how='left')

#Eliminar columnas innecesarias
df = df.drop(columns=['Lugar de nacemento', 'nombre'])

# Pivotar el DataFrame para tener una fila por combinación de Tempo, Espazo y grupo de edad, y columnas para cada Sexo
df_pivot = df.pivot_table(index=['Tempo', 'CodEspazo', 'id_lugar_nacimiento'], columns='Sexo', values='DatoN').reset_index()
df_pivot.rename(columns={'Tempo': 'id_fecha', 'CodEspazo': 'id_municipio', 'Homes': 'hombres', 'Mulleres': 'mujeres', 'Total': 'poblacion_total'}, inplace=True)

# Convertir id_fecha a formato DATE
df_pivot['id_fecha'] = pd.to_datetime(df_pivot['id_fecha'], format='%Y').dt.date

# Hacer un merge para insertar solo nuevos datos
existing_data = pd.read_sql('SELECT id_fecha, id_municipio FROM verin_dw.fact_poblacion_lugar', engine)
df_merged = df_pivot.merge(existing_data, on=['id_fecha', 'id_municipio'], how='left', indicator=True)
df_to_insert = df_merged[df_merged['_merge'] == 'left_only'].drop(columns=['_merge'])

# Insertar/actualizar en la tabla fact_poblacion_lugar
df_to_insert.to_sql('fact_poblacion_lugar', engine, schema='verin_dw', if_exists='append', index=False)