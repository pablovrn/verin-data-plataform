## Dashboard web

Este proyecto incluye un dashboard web estatico en `docs/index.html`, pensado para publicarse en GitHub Pages.

### Publicacion en GitHub Pages

1. Asegurate de tener el `.env` del proyecto con las mismas variables que usa `etl/carga_poblacion.py`.
2. Instala dependencias:
   `pip install -r requirements.txt`
3. Exporta los datos estaticos:
   `python3 procesos/export_dashboard_data.py`
4. Publica la carpeta `docs/` en GitHub Pages.

La web no expone credenciales en el navegador. Los datos se consultan desde PostgreSQL/Supabase solo durante la exportacion y se guardan en `docs/data/dashboard.json`.

### Desarrollo local

Si quieres previsualizar el sitio en local, puedes servir `docs/` con cualquier servidor estatico. Por ejemplo:

`python3 -m http.server 8000 --directory docs`
