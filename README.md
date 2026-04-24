# Lab 2: Weather Analytics Pipeline (Airflow + Snowflake + dbt + Preset)

## Overview

This project implements an end-to-end weather analytics pipeline with a focus on **ELT and visualization**. Weather data is ingested and transformed using **Apache Airflow** and **Snowflake**, modeled into analytical tables using **dbt**, and visualized using **Preset dashboards**. The pipeline demonstrates automated orchestration, idempotent ETL design, and interactive BI reporting.

**Flow:**
Open-Meteo Data → Airflow ETL → Snowflake Tables → dbt ELT Models → Preset Dashboards

---

## Objectives

* Build an Airflow ETL pipeline to load and transform weather data into Snowflake
* Implement **idempotent data loading** using SQL transactions and error handling
* Create **dbt models, tests, and snapshots** for analytical transformations
* Integrate dbt execution within the Airflow DAG
* Build **interactive dashboards in Preset** using dbt-generated tables

---

## Technologies Used

* Apache Airflow
* Snowflake
* dbt Core (`dbt-snowflake`)
* Preset (Apache Superset)
* Python
* SQL

---

## Data Pipeline Architecture

### ETL Layer (Airflow + Snowflake)

* **Raw Table:** `OPEN_METEO_RAW`
* **Transformed Tables:**

  * `WEATHER_OBSERVATION_HOURLY`
  * `WEATHER_DAILY`
  * `FORECAST_OUTPUT`
* **Final Table:**

  * `WEATHER_FINAL`

### ELT Layer (dbt)

* **Staging Model:**

  * `STG_WEATHER_FINAL`
* **Mart Models:**

  * `WEATHER_DAILY_SUMMARY`
  * `WEATHER_TEMP_TRENDS`
  * `WEATHER_LOCATION_SUMMARY`
* **Snapshot:**

  * `WEATHER_FINAL_SNAPSHOT`

---

## Airflow DAG

### DAG Name

```
lab2_etl_pipeline
```

### Task Flow

```
load_open_meteo_raw
    → transform_weather
    → build_weather_final
    → validate_weather_tables
    → dbt_run
    → dbt_test
    → dbt_snapshot
```

### Features

* Uses Airflow **Connections and Variables**
* Implements **SQL transaction + rollback**
* Ensures **idempotent execution**
* Executes dbt as part of DAG workflow

---

## Idempotency & Error Handling

* SQL transactions (`BEGIN`, `COMMIT`, `ROLLBACK`)
* Python `try/except` blocks
* Safe table rebuild strategy (no duplicate inserts)
* Validation checks on row counts and duplicates

---

## dbt Implementation

### Models

* **Staging Layer:** cleans raw final table
* **Mart Layer:** performs aggregations and analytics
* Includes:

  * daily averages
  * min/max temperatures
  * 7-day moving averages

### Commands Used

```bash
dbt run --profiles-dir .
dbt test --profiles-dir .
dbt snapshot --profiles-dir .
```

---

## Preset Dashboards

### 1. Temperature Trends Analysis

* Daily Average Temperature Trend
* 7-Day Moving Average Temperature

### 2. Temperature Distribution & Observations

* Daily Min/Max Temperature
* Daily Observation Count

### 3. Weather Summary Dashboard

* Average Temperature
* Minimum Temperature
* Maximum Temperature

### Interactivity

* Time-range filter (`event_date`)
* Dashboard updates based on selected date range

## Notes

* Dashboard focuses on **historical trends, variation, and moving averages**
* Filters demonstrate interactive data exploration

---

## Authors

* Priyank Mehta
* Vrishin Parambath
