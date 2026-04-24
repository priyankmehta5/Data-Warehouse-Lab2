# Lab 2: Weather Analytics using Airflow, Snowflake, dbt, and Preset

## Overview
This lab implements an end-to-end weather analytics pipeline with emphasis on ELT and visualization. Historical weather data is ingested and transformed using Apache Airflow and Snowflake, then modeled into analytical tables using dbt. The final transformed datasets are visualized in Preset dashboards to show temperature trends, moving averages, summary statistics, and observation distributions.

The project follows the required workflow:

**ETL with Airflow -> Snowflake tables -> ELT with dbt -> Visualization with Preset**

---

## Objectives
- Build an Airflow ETL pipeline to populate raw and transformed weather tables in Snowflake
- Implement idempotent loading logic with SQL transactions and error handling
- Build dbt models, tests, and snapshots on top of the final weather table
- Schedule dbt execution as part of the Airflow DAG
- Visualize transformed results in Preset using multiple dashboards and interactive date filters

---

## Technologies Used
- Apache Airflow
- Snowflake
- dbt Core with dbt-snowflake
- Preset
- Python
- SQL

---

## Data Pipeline Architecture
1. **Data Ingestion**
   - Weather data is loaded into `OPEN_METEO_RAW`
2. **Transformation in ETL**
   - Intermediate transformed tables are created:
     - `WEATHER_OBSERVATION_HOURLY`
     - `WEATHER_DAILY`
     - `FORECAST_OUTPUT`
3. **Final ETL Output**
   - Final merged table:
     - `WEATHER_FINAL`
4. **ELT with dbt**
   - dbt models are built on top of `WEATHER_FINAL` to produce analytical tables
5. **Visualization**
   - Preset dashboards visualize daily trends, min/max ranges, moving averages, and summary metrics

---

## Snowflake Tables

### ETL Tables
- `OPEN_METEO_RAW`
- `WEATHER_OBSERVATION_HOURLY`
- `WEATHER_DAILY`
- `FORECAST_OUTPUT`
- `WEATHER_FINAL`

### dbt Models
- `STG_WEATHER_FINAL`
- `WEATHER_DAILY_SUMMARY`
- `WEATHER_TEMP_TRENDS`
- `WEATHER_LOCATION_SUMMARY`

### dbt Snapshot
- `WEATHER_FINAL_SNAPSHOT`

---

## Airflow DAG
The Airflow DAG used in this lab is:

```text
lab2_etl_pipeline
