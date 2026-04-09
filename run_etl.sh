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

for script in etl/*.py; do
    echo "Ejecutando $script..."
    python "$script"
done

echo "Proceso completado"