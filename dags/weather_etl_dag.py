"""
Weather ETL Pipeline DAG (Airflow 3.2.0 compatible)
"""

from datetime import datetime, timedelta, timezone
import json
import os
import sqlite3

import pandas as pd
import requests

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
CITIES = {
    "Karachi":   {"latitude": 24.8608,  "longitude": 67.0104},
    "London":    {"latitude": 51.5074,  "longitude": -0.1278},
    "New York":  {"latitude": 40.7128,  "longitude": -74.0060},
    "Tokyo":     {"latitude": 35.6762,  "longitude": 139.6503},
}

DATA_DIR   = "/opt/airflow/data"
RAW_FILE   = os.path.join(DATA_DIR, "raw_weather.json")
CLEAN_FILE = os.path.join(DATA_DIR, "daily_weather_summary.csv")
DB_FILE    = os.path.join(DATA_DIR, "weather.db")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _weather_label(row):
    if row["total_precip_mm"] > 5:
        return "Rainy"
    elif row["avg_temp_c"] > 30:
        return "Hot"
    elif row["avg_temp_c"] < 5:
        return "Cold"
    return "Mild"


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract_weather(**context):
    os.makedirs(DATA_DIR, exist_ok=True)
    all_data = {}

    for city, coords in CITIES.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "hourly": "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m",
            "forecast_days": 7,
            "timezone": "UTC",
        }

        print(f"[EXTRACT] Fetching {city}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        all_data[city] = response.json()

    # Atomic write: prevents stale/partial JSON if a previous run's file exists
    tmp_path = RAW_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(all_data, f, indent=2)
    os.replace(tmp_path, RAW_FILE)

    return RAW_FILE


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------
def transform_weather(**context):
    with open(RAW_FILE) as f:
        raw = json.load(f)

    frames = []

    for city, payload in raw.items():
        hourly = payload["hourly"]

        df = pd.DataFrame({
            "datetime": pd.to_datetime(hourly["time"]),
            "temperature": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],
            "windspeed": hourly["windspeed_10m"],
            "humidity": hourly["relativehumidity_2m"],
        })

        df["date"] = df["datetime"].dt.date

        daily = df.groupby("date").agg(
            avg_temp_c=("temperature", "mean"),
            max_temp_c=("temperature", "max"),
            min_temp_c=("temperature", "min"),
            total_precip_mm=("precipitation", "sum"),
            avg_windspeed=("windspeed", "mean"),
            avg_humidity=("humidity", "mean"),
        ).reset_index()

        daily["city"] = city
        daily["weather_label"] = daily.apply(_weather_label, axis=1)

        frames.append(daily)

    result = pd.concat(frames, ignore_index=True)
    result["pipeline_run_at"] = datetime.now(timezone.utc).isoformat()

    tmp_path = CLEAN_FILE + ".tmp"
    result.to_csv(tmp_path, index=False)
    os.replace(tmp_path, CLEAN_FILE)
    return CLEAN_FILE


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------
def load_weather(**context):
    df = pd.read_csv(CLEAN_FILE)

    conn = sqlite3.connect(DB_FILE)
    df.to_sql("daily_weather", conn, if_exists="replace", index=False)
    conn.close()

    print(f"[LOAD] Inserted {len(df)} rows into SQLite")


# ---------------------------------------------------------------------------
# REPORT
# ---------------------------------------------------------------------------
def generate_report(**context):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM daily_weather", conn)
    conn.close()

    print("\n=== WEATHER SUMMARY REPORT ===\n")

    for city in df["city"].unique():
        city_df = df[df["city"] == city]
        print(f"{city}")
        print(f" Avg Temp: {city_df['avg_temp_c'].mean():.1f}")
        print(f" Max Temp: {city_df['max_temp_c'].max():.1f}")
        print(f" Min Temp: {city_df['min_temp_c'].min():.1f}\n")


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="Open-Meteo ETL Pipeline (Airflow 3)",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",   # ✅ UPDATED (IMPORTANT CHANGE)
    catchup=False,
    tags=["etl", "weather"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    extract >> transform >> load >> report