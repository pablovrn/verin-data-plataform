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
pip install --upgrade pip -q
pip install -r requirements.txt -q

# Si se le pasa un argumento, se ejecuta solo ese script
if [ $# -gt 0 ]; then
    script="$1"
    if [ -f "etl/$script" ]; then
        echo "Ejecutando $script..."
        python "etl/$script"
    else
        echo "El script $script no existe en la carpeta etl."
    fi
else
    for script in etl/*.py; do
        echo "Ejecutando $script..."
        python "$script"
    done
fi

echo "Proceso completado"