# Weather ETL Pipeline — Apache Airflow + Docker

A hands-on classroom project demonstrating a complete **Extract → Transform → Load** pipeline
using **Apache Airflow 2.8** running in Docker. No API keys required.

---

## Table of Contents

1. [What This Project Does](#what-this-project-does)
2. [Prerequisites](#prerequisites)
3. [Project Structure](#project-structure)
4. [Step-by-Step Setup](#step-by-step-setup)
5. [Running the Pipeline](#running-the-pipeline)
6. [Viewing the Output](#viewing-the-output)
7. [Understanding the DAG Code](#understanding-the-dag-code)
8. [Output Data Schema](#output-data-schema)
9. [Troubleshooting](#troubleshooting)
10. [Stopping the Project](#stopping-the-project)

---

## What This Project Does

```
Open-Meteo Weather API  (free, no API key)
           │
           ▼
    ┌─────────────┐
    │   EXTRACT   │  Fetches 7-day hourly forecast for 4 cities
    └──────┬──────┘  → saves raw_weather.json
           │
           ▼
    ┌─────────────┐
    │  TRANSFORM  │  Aggregates hourly data into daily summaries
    └──────┬──────┘  → saves daily_weather_summary.csv
           │
           ▼
    ┌─────────────┐
    │    LOAD     │  Writes final data into SQLite database
    └──────┬──────┘  → saves weather.db
           │
           ▼
    ┌─────────────┐
    │   REPORT    │  Prints a formatted summary to the task logs
    └─────────────┘
```

**Cities covered:** Karachi · London · New York · Tokyo

![architectiure-airflow](./architectiure-airflow.png)

---

## Prerequisites

Make sure the following are installed before starting:

| Tool | How to check | Download |
|------|-------------|----------|
| Docker Desktop | `docker --version` | https://www.docker.com/products/docker-desktop |
| Docker Compose | `docker compose version` | Included with Docker Desktop |
| Git (optional) | `git --version` | https://git-scm.com |

> **Windows users:** Docker Desktop requires **WSL 2**. Enable it via:
> `wsl --install` in PowerShell (run as Administrator), then restart.

---

## Project Structure

```
airflow-etl-project/
│
├── docker-compose.yaml          # Defines Airflow + Postgres services
├── Dockerfile                   # Custom Airflow image with pandas & requests pre-installed
├── .env                         # Sets AIRFLOW_UID (required on Linux/Mac)
├── README.md                    # This file
│
├── dags/
│   └── weather_etl_dag.py       # The ETL pipeline — 4 tasks
│
├── data/                        # Output files are written here
│   ├── raw_weather.json         # Created after Extract task runs
│   ├── daily_weather_summary.csv# Created after Transform task runs
│   └── weather.db               # Created after Load task runs
│
├── logs/                        # Airflow task execution logs
└── plugins/                     # Custom Airflow plugins (empty for now)
```

---

## Step-by-Step Setup

### Step 1 — Clone or download the project

```bash
# If using git:
git clone <repo-url>
cd airflow-etl-project

# Or just navigate to the folder:
cd airflow-etl-project
```

### Step 2 — Verify Docker is running

```bash
docker --version
# Expected output: Docker version 24.x.x or higher

docker compose version
# Expected output: Docker Compose version v2.x.x
```

If Docker is not running, open **Docker Desktop** and wait for the whale icon to stop animating.

### Step 3 — Build the custom image and start all services

```bash
docker compose build
docker compose up -d
```

`docker compose build` is only needed **once** (or after changing the Dockerfile). It builds a custom Airflow image with `pandas` and `requests` pre-installed, so containers start up fast every time.

This will:
- Build the custom image based on `apache/airflow:2.8.1` with required packages
- Pull the `postgres:15` image (~600 MB, first time only)
- Initialize the Airflow database
- Create the `admin` user automatically
- Start the webserver and scheduler in the background

**Wait about 30–60 seconds** for everything to be ready.

### Step 4 — Confirm services are healthy

```bash
docker compose ps
```

You should see all services with status `running` or `healthy`:

```
NAME                                          STATUS
airflow-etl-project-postgres-1               running (healthy)
airflow-etl-project-airflow-init-1           Exited
airflow-etl-project-airflow-webserver-1      running (healthy)
airflow-etl-project-airflow-scheduler-1      running
```

> `airflow-init` showing `Exited` is **normal and expected** — it runs once to set up the database and create the admin user, then exits cleanly.

---

## Running the Pipeline

### Step 5 — Open the Airflow Web UI

Open your browser and go to:

```
http://localhost:8080
```

Login with:
- **Username:** `admin`
- **Password:** `admin`

### Step 6 — Find the DAG

On the **DAGs** page you will see **`weather_etl_pipeline`**.

> If the DAG is not visible yet, wait 30 seconds and refresh. The scheduler scans for new DAGs periodically.

### Step 7 — Enable the DAG

Click the **toggle switch** on the left side of the DAG row to turn it **ON** (it turns blue).

### Step 8 — Trigger a manual run

1. Click on the DAG name **`weather_etl_pipeline`** to open it
2. Click the **▶ Trigger DAG** button (top right, looks like a play button)
3. Click **Trigger** to confirm

### Step 9 — Watch the pipeline execute

In the **Graph View**, you will see 4 task boxes:

```
extract_weather_data  →  transform_weather_data  →  load_to_sqlite  →  generate_report
```

Colors indicate status:
- **Light green (running)** — task is currently executing
- **Dark green (success)** — task completed successfully
- **Red (failed)** — task failed (click it → View Logs to debug)

The full pipeline takes about **15–30 seconds** to complete.

### Step 10 — View task logs

Click on any task box → **Log** to see detailed output including:
- Which cities were fetched
- How many rows were processed
- The final summary report (in the `generate_report` log)

---

## Viewing the Output

After a successful run, three output files are created in the `data/` folder.

### View the raw JSON (extracted data)

```bash
# On Mac/Linux:
cat data/raw_weather.json | head -60

# On Windows (PowerShell):
Get-Content data\raw_weather.json | Select-Object -First 60
```

### View the CSV (transformed data)

```bash
# On Mac/Linux:
cat data/daily_weather_summary.csv

# On Windows (PowerShell):
Get-Content data\daily_weather_summary.csv
```

Expected output:
```
date,city,avg_temp_c,max_temp_c,min_temp_c,total_precip_mm,avg_windspeed,avg_humidity,weather_label,pipeline_run_at
2024-01-15,Karachi,28.4,33.1,24.2,0.0,14.3,62.5,Hot,2024-01-15T10:30:00
2024-01-15,London,8.2,11.5,5.1,3.2,22.7,81.0,Rainy,2024-01-15T10:30:00
...
```

### Query the SQLite database (loaded data)

```bash
# On Mac/Linux (requires sqlite3 installed):
sqlite3 data/weather.db "SELECT city, date, avg_temp_c, weather_label FROM daily_weather ORDER BY city, date;"

# From inside Docker (works everywhere):
docker compose exec airflow-webserver sqlite3 /opt/airflow/data/weather.db \
  "SELECT city, date, avg_temp_c, weather_label FROM daily_weather ORDER BY city, date;"
```

---

## Understanding the DAG Code

Open [dags/weather_etl_dag.py](dags/weather_etl_dag.py) and follow along:

### DAG Definition
```python
with DAG(
    dag_id="weather_etl_pipeline",   # Unique name shown in the UI
    schedule_interval="@daily",       # Runs automatically once per day
    catchup=False,                    # Don't backfill missed runs
    ...
) as dag:
```

### Task 1 — Extract
```python
t_extract = PythonOperator(
    task_id="extract_weather_data",
    python_callable=extract_weather,  # Calls this Python function
)
```
Calls `requests.get()` against the Open-Meteo API for each city and saves raw JSON.

### Task 2 — Transform
```python
t_transform = PythonOperator(
    task_id="transform_weather_data",
    python_callable=transform_weather,
)
```
Uses `pandas` to group hourly records by date and compute daily statistics.

### Task 3 — Load
```python
t_load = PythonOperator(
    task_id="load_to_sqlite",
    python_callable=load_weather,
)
```
Uses `pandas.to_sql()` to write the cleaned DataFrame into a SQLite table.

### Task Dependencies
```python
t_extract >> t_transform >> t_load >> t_report
```
The `>>` operator defines execution order. Each task only starts after the previous one succeeds.

---

## Output Data Schema

Table name: `daily_weather` (in `weather.db`)

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Forecast date |
| `city` | TEXT | City name |
| `avg_temp_c` | FLOAT | Average temperature (°C) |
| `max_temp_c` | FLOAT | Maximum temperature (°C) |
| `min_temp_c` | FLOAT | Minimum temperature (°C) |
| `total_precip_mm` | FLOAT | Total precipitation (mm) |
| `avg_windspeed` | FLOAT | Average wind speed (km/h) |
| `avg_humidity` | FLOAT | Average relative humidity (%) |
| `weather_label` | TEXT | Rainy / Hot / Cold / Mild |
| `pipeline_run_at` | TEXT | UTC timestamp of pipeline run |

---

## Troubleshooting

### DAG not appearing in the UI
```bash
# Check scheduler logs for import errors in the DAG file
docker compose logs airflow-scheduler | grep ERROR
```

### A task turns red (failed)
1. Click the red task box in Graph View
2. Click **Log**
3. Scroll to the bottom to see the error message

### Port 8080 already in use
Edit `docker-compose.yaml` and change `"8080:8080"` to `"8081:8080"`, then access the UI at `http://localhost:8081`.

### Services not starting / unhealthy
```bash
# View all service logs
docker compose logs

# Restart everything cleanly
docker compose down
docker compose build
docker compose up -d
```

### Reset everything (wipe DB and start fresh)
```bash
docker compose down -v        # -v removes the postgres volume
rm -f data/*.json data/*.csv data/*.db
docker compose up -d
```

---

## Stopping the Project

```bash
# Stop containers but keep data
docker compose stop

# Stop and remove containers (keeps the postgres volume)
docker compose down

# Stop, remove containers AND delete all data (full reset)
docker compose down -v
```
