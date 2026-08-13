#!/bin/bash

# Crear entorno virtual si no existe
if [ ! -d "venv" ]; then
    echo "Creando entorno virtual..."
    python3 -m venv venv
else
    echo "El entorno virtual ya existe."
fi

# Activar entorno
echo "Activando entorno virtual..."
source venv/bin/activate

# Instalar dependencias
echo "Instalando dependencias..."
pip install --upgrade pip -q
pip install -r requirements.txt -q

# Arrancar la API
echo "Iniciando API..."

uvicorn ia.app:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload

echo "API detenida."