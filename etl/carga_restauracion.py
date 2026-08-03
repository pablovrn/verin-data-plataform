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
ETL para cargar la tabla de restauración en la base de datos verin_dw.
fuente de datos: Scrapeo de Google Maps con Apify (data/dat_bar_restuarante_verin.json)
"""
try:
    # Leer el archivo JSON
    df = pd.read_json('data/data_bar_restaurante_verin.json')
    df = df[['title', 'categoryName', 'street', 'phoneUnformatted', 'totalScore', 'reviewsCount', 'permanentlyClosed', 'temporarilyClosed', 'url']]

    # Mappear las columnas del DataFrame a los nombres de las columnas de la tabla en la base de datos
    MAPEO = {
        # RESTAURANTES
        "Restaurante": "Restaurante",
        "Restaurante de comida para llevar": "Restaurante",
        "Restaurante especializado en tapas": "Restaurante",
        "Restaurante familiar": "Restaurante",
        "Restaurante gallego": "Restaurante",
        "Restaurante turco": "Restaurante",
        "Pizzería": "Restaurante",
        "Hamburguesería": "Restaurante",

        # BARES
        "Bar": "Bar",
        "Bar de tapas": "Bar",
        "Bar musical": "Bar",
        "Bar restaurante": "Bar",
        "Bar de alterne": "Bar",
        "Pub": "Bar",
        "Coctelería": "Bar",
        "Tienda de vinos": "Bar",
        "Club nocturno": "Bar",

        # CAFETERÍAS
        "Cafetería": "Cafetería",
        "Restaurante o cafetería": "Cafetería",
        "Churrería": "Cafetería",
        "Pastelería": "Cafetería",
        "Panadería": "Cafetería",
        "Heladería": "Cafetería",
        "Bufé de dulces y repostería": "Cafetería",

        # OTROS
        "Agencia de apuestas": "Otros",
        "Casa de apuestas": "Otros",
        "Comercio": "Otros",
        "Estanco": "Otros",
        "Gasolinera": "Otros",
        "Hotel": "Otros",
        "Mayorista": "Otros",
        "Parque infantil": "Otros",
        "Piscina pública": "Otros",
        "Supermercado": "Otros",
        "Bodega": "Otros",
        "Jamonería": "Otros",
        "Tienda de bricolaje": "Otros",
    }

    df["tipo_negocio"] = (
        df["categoryName"]
        .map(MAPEO)
        .fillna("Otros")
    )

    tipos_dw = pd.read_sql("SELECT id, nombre FROM verin_dw.dim_tipo_negocio", engine)
    df = df.merge(tipos_dw, left_on='tipo_negocio', right_on='nombre', how='left').drop(columns=['tipo_negocio', 'nombre']) 

    # Mapear la columna "permanentlyClosed" y "temporarilyClosed" a una columna "abierto" con valores booleanos
    df["abierto"] = ~df["permanentlyClosed"].fillna(False) & ~df["temporarilyClosed"].fillna(False)

    # Borrar columnas innecesarias
    df = df.drop(columns=['categoryName', 'permanentlyClosed', 'temporarilyClosed'])

    # Renombrar columnas para que coincidan con la tabla fact_restauracion
    df = df.rename(columns={
        'title': 'nombre',
        'street': 'direccion',
        'phoneUnformatted': 'contacto',
        'totalScore': 'rating',
        'reviewsCount': 'numero_valoraciones',
        'url': 'url',
        'id': 'tipo'})

    # Truncar la tabla data_restauracion antes de insertar nuevos datos
    with engine.connect() as connection:
        connection.execute(text("TRUNCATE TABLE verin_dw.data_restauracion"))
        connection.commit()

    # Insertar datos en la tabla data_restauracion
    df.to_sql('data_restauracion', engine, schema='verin_dw', if_exists='append', index=False)
    print("ETL de data_restauracion completado con éxito.")

except Exception as e:
    print(f"Error en el ETL de data_restauracion: {e}")
    exit(1)