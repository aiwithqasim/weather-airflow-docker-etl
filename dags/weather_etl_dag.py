"""
Weather ETL Pipeline DAG
========================
Extracts hourly weather forecast data from Open-Meteo (free, no API key),
transforms it into daily summaries, and loads to CSV + SQLite.

API Docs: https://open-meteo.com/en/docs
"""

from datetime import datetime, timedelta
import json
import os
import sqlite3

import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

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
# Task 1 – EXTRACT
# ---------------------------------------------------------------------------
def extract_weather(**context):
    """Fetch hourly weather data for all cities from Open-Meteo API."""
    os.makedirs(DATA_DIR, exist_ok=True)
    all_data = {}

    for city, coords in CITIES.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude":            coords["latitude"],
            "longitude":           coords["longitude"],
            "hourly":              "temperature_2m,precipitation,windspeed_10m,relativehumidity_2m",
            "forecast_days":       7,
            "timezone":            "UTC",
        }

        print(f"[EXTRACT] Fetching data for {city}...")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        all_data[city] = response.json()
        print(f"[EXTRACT] {city}: {len(response.json()['hourly']['time'])} hourly records fetched.")

    with open(RAW_FILE, "w") as f:
        json.dump(all_data, f, indent=2)

    print(f"[EXTRACT] Raw data saved to {RAW_FILE}")
    return RAW_FILE


# ---------------------------------------------------------------------------
# Task 2 – TRANSFORM
# ---------------------------------------------------------------------------
def transform_weather(**context):
    """
    Parse hourly data → aggregate to daily summaries per city.
    Computes: avg/max/min temperature, total precipitation,
              avg wind speed, avg humidity.
    """
    with open(RAW_FILE) as f:
        raw = json.load(f)

    daily_records = []

    for city, payload in raw.items():
        hourly = payload["hourly"]

        df = pd.DataFrame({
            "datetime":    pd.to_datetime(hourly["time"]),
            "temperature": hourly["temperature_2m"],
            "precipitation": hourly["precipitation"],
            "windspeed":   hourly["windspeed_10m"],
            "humidity":    hourly["relativehumidity_2m"],
        })

        df["date"] = df["datetime"].dt.date

        daily = (
            df.groupby("date")
            .agg(
                avg_temp_c    =("temperature",   "mean"),
                max_temp_c    =("temperature",   "max"),
                min_temp_c    =("temperature",   "min"),
                total_precip_mm=("precipitation", "sum"),
                avg_windspeed  =("windspeed",     "mean"),
                avg_humidity   =("humidity",      "mean"),
            )
            .reset_index()
        )

        daily["city"] = city
        daily["avg_temp_c"]     = daily["avg_temp_c"].round(2)
        daily["max_temp_c"]     = daily["max_temp_c"].round(2)
        daily["min_temp_c"]     = daily["min_temp_c"].round(2)
        daily["total_precip_mm"]= daily["total_precip_mm"].round(2)
        daily["avg_windspeed"]  = daily["avg_windspeed"].round(2)
        daily["avg_humidity"]   = daily["avg_humidity"].round(1)

        # Add a simple weather label based on temp + precip
        daily["weather_label"] = daily.apply(_weather_label, axis=1)

        daily_records.append(daily)
        print(f"[TRANSFORM] {city}: {len(daily)} daily summaries created.")

    result = pd.concat(daily_records, ignore_index=True)
    result["pipeline_run_at"] = datetime.utcnow().isoformat()

    result.to_csv(CLEAN_FILE, index=False)
    print(f"[TRANSFORM] Cleaned data saved to {CLEAN_FILE}")
    print(result.to_string(index=False))
    return CLEAN_FILE


def _weather_label(row):
    if row["total_precip_mm"] > 5:
        return "Rainy"
    elif row["avg_temp_c"] > 30:
        return "Hot"
    elif row["avg_temp_c"] < 5:
        return "Cold"
    else:
        return "Mild"


# ---------------------------------------------------------------------------
# Task 3 – LOAD
# ---------------------------------------------------------------------------
def load_weather(**context):
    """Load the daily summaries into a SQLite database table."""
    df = pd.read_csv(CLEAN_FILE)

    conn = sqlite3.connect(DB_FILE)
    df.to_sql("daily_weather", conn, if_exists="replace", index=False)
    conn.close()

    print(f"[LOAD] {len(df)} rows written to SQLite: {DB_FILE}")
    print(f"[LOAD] Table: daily_weather")

    # Verify
    conn = sqlite3.connect(DB_FILE)
    count = conn.execute("SELECT COUNT(*) FROM daily_weather").fetchone()[0]
    sample = pd.read_sql("SELECT city, date, avg_temp_c, weather_label FROM daily_weather LIMIT 8", conn)
    conn.close()

    print(f"[LOAD] Verified: {count} rows in DB.")
    print(sample.to_string(index=False))


# ---------------------------------------------------------------------------
# Task 4 – REPORT (bonus)
# ---------------------------------------------------------------------------
def generate_report(**context):
    """Print a simple summary report for the class to see."""
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT * FROM daily_weather", conn)
    conn.close()

    print("\n" + "="*60)
    print("         WEATHER ETL PIPELINE – SUMMARY REPORT")
    print("="*60)

    for city in df["city"].unique():
        city_df = df[df["city"] == city]
        print(f"\n  {city}:")
        print(f"    Avg Temperature : {city_df['avg_temp_c'].mean():.1f} °C")
        print(f"    Max Temperature : {city_df['max_temp_c'].max():.1f} °C")
        print(f"    Min Temperature : {city_df['min_temp_c'].min():.1f} °C")
        print(f"    Total Rainfall  : {city_df['total_precip_mm'].sum():.1f} mm")
        label_counts = city_df["weather_label"].value_counts().to_dict()
        print(f"    Weather Labels  : {label_counts}")

    print("\n" + "="*60)
    print(f"  Output files:")
    print(f"    Raw JSON  : {RAW_FILE}")
    print(f"    CSV       : {CLEAN_FILE}")
    print(f"    SQLite DB : {DB_FILE}")
    print("="*60 + "\n")


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
default_args = {
    "owner":            "airflow",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_etl_pipeline",
    description="ETL: Open-Meteo API → Transform → SQLite + CSV",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",       # runs once per day; trigger manually in class
    catchup=False,
    tags=["etl", "weather", "demo"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather,
    )

    t_transform = PythonOperator(
        task_id="transform_weather_data",
        python_callable=transform_weather,
    )

    t_load = PythonOperator(
        task_id="load_to_sqlite",
        python_callable=load_weather,
    )

    t_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_report,
    )

    # Pipeline order: Extract → Transform → Load → Report
    t_extract >> t_transform >> t_load >> t_report
