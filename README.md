# VERÍN ABERTO

Verín Aberto es una plataforma de datos que centraliza y almacena estadísticas sobre la comarca de Verín (provincia de Ourense).

La plataforma utiliza PostgreSQL como base de datos y carga la información mediante procesos ETL desarrollados en Python, apoyándose en la librería Pandas para la transformación y tratamiento de los datos.

> **Estado del proyecto:** 🚧 Ampliación.

## Fuentes de datos
Actualmente la plataforma integra información procedente de:

* **IGE (Instituto Galego de Estatística)**
* **Google Maps**, mediante procesos de scraping ejecutados con Apify

## Ámbitos de información
Actualmente la plataforma dispone de información sobre:

* Población
* Economía
* Restauración
* Servicios (Sanidad y Educación)

## Estructura del proyecto

```
.
├── data/                  # Datos estáticos
├── docs/                  # Dashboard web
│   ├── css/
│   ├── data/
│   ├── images/
│   ├── js/
│   └── index.html
├── etl/                   # ETL
├── ia/                    # API Asistente de IA
├── procesos/              # Procesos de ejecución
├── sql/                   # Esquema y consultas SQL
│   └── datamarts/
├── visualizacion/         # Dashboards de Tableau
├── requirements.txt
└── README.md
```

## Instalación

Clona el repositorio:

```bash
git clone https://github.com/pablovrn/verin-data-plataform.git
cd verin-data-plataform
```

Configura las variables de entorno creando un archivo `.env` con las credenciales necesarias para acceder a la base de datos.

## Ejecución

El proyecto incluye un script que automatiza la creación del entorno virtual, la instalación de dependencias y la ejecución de las ETL.

### Ejecutar todas las ETL

```bash
bash procesos/run_etl.sh
```

El script realiza automáticamente las siguientes tareas:

1. Crea el entorno virtual (`venv`) si no existe.
2. Activa el entorno virtual.
3. Actualiza `pip`.
4. Instala las dependencias de `requirements.txt`.
5. Ejecuta todas las ETL de la carpeta `etl/`.

### Ejecutar una única ETL

También es posible ejecutar únicamente una ETL indicando su nombre como argumento.

Por ejemplo:

```bash
bash procesos/run_etl.sh carga_poblacion.py
```

o

```bash
bash procesos/run_etl.sh carga_economia.py
```

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

## Asistente de IA

El proyecto incluye un asistente conversacional basado en inteligencia artificial, diseñado para permitir consultas en lenguaje natural sobre los datos almacenados en la plataforma.

El asistente utiliza un enfoque de **Text-to-SQL**, mediante el cual transforma la pregunta del usuario en una consulta SQL, valida dicha consulta, la ejecuta sobre PostgreSQL y posteriormente genera una respuesta en lenguaje natural a partir de los resultados obtenidos.

### Arquitectura

El flujo de una consulta es el siguiente:

```text
Pregunta del usuario
        │
        ▼
   Modelo de IA
        │
        ▼
    Generación SQL
        │
        ▼
   Validación SQL
        │
        ▼
     PostgreSQL
        │
        ▼
      Resultados
        │
        ▼
   Modelo de IA
        │
        ▼
 Respuesta en lenguaje natural
 ```

 La IA se ejecuta actualmente de forma local mediante Ollama, utilizando un modelo de lenguaje local.

 ### Estado actual
 El asistente se encuentra actualmente en fase experimental y de desarrollo.

Aunque el sistema ya es capaz de realizar consultas y generar respuestas correctamente en determinados casos, todavía presenta limitaciones en la interpretación de preguntas más complejas y en la generación de consultas para determinados escenarios.

Por este motivo, la funcionalidad de chat de IA se encuentra temporalmente desactivada en el dashboard web público.

La API y el asistente pueden ejecutarse y probarse de forma local durante el desarrollo.

Para ejecutar la API en local se utiliza el comando:

```bash
./procesos/run_api.sh
```

Para interactuar con ella se puede o bien acceder a http://localhost:8000/docs o bien descomentar el botón en index.html y levantar la web.


## Tecnologías

- Python
- Pandas
- PostgreSQL
- SQL
- Git
- Ollama
- FastAPI