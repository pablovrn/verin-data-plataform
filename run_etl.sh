#!/bin/bash

if [ ! -d "venv" ]; then
    echo "Creando entorno virtual..."
    python3 -m venv venv
else
    echo "El entorno virtual ya existe."
fi

echo "Activando entorno virtual..."
source venv/bin/activate

echo "Instalando librerías..."
pip install --upgrade pip
pip install -r requirements.txt


if [ -f "etl/poblacion.py" ]; then
    echo "Ejecutando ETL de población..."
    python etl/poblacion.py
else
    echo "No se encontró etl/poblacion.py"
fi

echo "Proceso completado"