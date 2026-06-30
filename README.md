# End-to-End Data Engineering Pipelines with Airflow, PostgreSQL & Docker

## Overview

This repository contains two end-to-end Data Engineering projects built
with **Python**, **Apache Airflow**, **PostgreSQL**, and **Docker**. The
pipelines ingest data from REST APIs and Excel files, process them using
the **Medallion Architecture (Bronze, Silver, Gold)**, and orchestrate
workflows with Apache Airflow.

## Architecture

``` text
REST API / Excel
       │
       ▼
 Bronze Layer
       │
       ▼
 Silver Layer
       │
       ▼
 Gold Layer
       │
       ▼
 PostgreSQL
       │
       ▼
 Apache Airflow
```

## Tech Stack

-   Python
-   Apache Airflow
-   PostgreSQL
-   Docker & Docker Compose
-   Pandas
-   SQLAlchemy
-   Requests
-   SQL

## Repository Structure

``` text
.
├── airflow/
│   ├── dags/
│   ├── logs/
│   └── plugins/
├── my_files/
│   └── Survey Data.xlsx
├── scripts/
│   ├── customer_pipeline.py
│   ├── survey_pipeline.py
│   └── loader.py
├── screenshots/
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Medallion Architecture

| Layer  | Purpose                                                              |
|--------|-----------------------------------------------------------------------|
| Bronze | Stores raw data extracted from APIs and Excel files.                 |
| Silver | Cleans, standardizes, validates, and enriches the data.               |
| Gold   | Creates aggregated, analytics-ready datasets for reporting and dashboards. |

# Project 1 -- Customer Cart Pipeline

## Objective

Build an ETL pipeline that extracts customer and cart data from the
DummyJSON API, enriches the data, and produces business-ready reporting
tables.

### Pipeline

-   Extract customer records
-   Extract cart records
-   Load raw data into Bronze
-   Clean and enrich customer/cart data in Silver
-   Generate Gold-layer summary tables
-   Orchestrate using Apache Airflow

### Screenshots

**Airflow DAG**

![Project 1 Airflow DAG](screenshots/Project1_Airflow%20DAG.png)

**Successful DAG Run**

![Project 1 Successful DAG Run](screenshots/Project1_Successful%20DAG%20run.png)

**Bronze Tables**

![Project 1 Bronze Table 1](screenshots/Project1_Bronze%20table1.png)
![Project 1 Bronze Table 2](screenshots/Project1_Bronze%20table2.png)

**Silver Table**

![Project 1 Silver Table](screenshots/Project1_Silver%20table.png)

**Gold Tables**

![Project 1 Gold Table 1](screenshots/Project1_Gold%20table1.png)
![Project 1 Gold Table 2](screenshots/Project1_Gold%20table2.png)
![Project 1 Gold Table 3](screenshots/Project1_Gold%20table3.png)
![Project 1 Gold Table 4](screenshots/Project1_Gold%20table4.png)

------------------------------------------------------------------------

# Project 2 -- Survey Data Pipeline

## Objective

Build a batch ETL pipeline that ingests salary survey data from Excel
and transforms it into analytics-ready datasets.

### Pipeline

-   Read survey data from Excel
-   Load raw records into Bronze
-   Rename and standardize columns
-   Load transformed records into Silver
-   Generate salary summary tables in Gold
-   Schedule with Apache Airflow

### Screenshots

**Airflow DAG**

![Project 2 Airflow DAG](screenshots/Project2_%20Airflow%20DAG.png)

**Successful DAG Run**

![Project 2 Successful DAG Run](screenshots/Project2_%20Successful%20DAG%20run.png)

**Bronze Table**

![Project 2 Bronze Table](screenshots/Project2_Bronze%20table.png)

**Silver Table**

![Project 2 Silver Table](screenshots/Project2_Silver%20table.png)

**Gold Tables**

![Project 2 Gold Table 1](screenshots/Project2_Gold%20table1.png)
![Project 2 Gold Table 2](screenshots/Project2_Gold%20table2.png)
![Project 2 Gold Table 3](screenshots/Project2_Gold%20table3.png)

------------------------------------------------------------------------

# Features

-   REST API ingestion
-   Excel file ingestion
-   Bronze, Silver and Gold layers
-   Medallion Architecture
-   Apache Airflow orchestration
-   PostgreSQL storage
-   Dockerized deployment
-   Data cleaning and validation
-   Business-ready aggregated tables

------------------------------------------------------------------------

# Getting Started

## Prerequisites

-   Docker
-   Docker Compose
-   Git

## Clone Repository

``` bash
git clone https://github.com/Sanusi-Abdulmalik/wesonline-data-engineering-tutorial  
cd wesonline-data-engineering-tutorial
```

## Run

``` bash
docker compose up --build
```

### Airflow

-   URL: http://localhost:8081
-   Username: admin
-   Password: admin

### pgAdmin

-   URL: http://localhost:8080

Register a server using:

-   Host: postgres
-   Port: 5432
-   Database: airflow
-   Username: airflow
-   Password: airflow

------------------------------------------------------------------------

# Future Improvements

-   Incremental loading
-   Slowly Changing Dimensions (SCD)
-   Data quality checks
-   dbt transformations
-   GitHub Actions CI/CD
-   Power BI dashboards

------------------------------------------------------------------------

# Authors
-   [Abdulmalik Sanusi](https://github.com/Sanusi-Abdulmalik)
-   [Bright Osas](https://github.com/BrightOsas)

Created as a portfolio project demonstrating modern Data Engineering
practices using Python, Apache Airflow, PostgreSQL, Docker, and the
Medallion Architecture.
