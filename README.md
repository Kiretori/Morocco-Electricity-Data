# Morocco Electricity Data Pipeline

Analytics pipeline for Morocco's electricity sector using Airflow and dbt.

## Overview

Processes electricity generation, demand, emissions, and import data for Morocco. Built with dbt transformations orchestrated through Apache Airflow.

## Structure

- `dags/` - Airflow DAGs for pipeline orchestration
- `dbt_electricity/` - dbt project with staging and mart models
- `models/staging/` - Raw data cleaning and standardization
- `models/marts/` - Business logic and final analytics tables

## Key Datasets

- Electricity generation by source
- Energy demand patterns
- CO2 emissions data
- Import/export flows

## Setup

1. Configure database connection in Airflow
2. Set required variables in Airflow UI
3. Deploy with Astro CLI: `astro dev start`

## Models

Pipeline generates yearly and detailed views of Morocco's electricity metrics including generation mix, emissions intensity, and demand forecasting.