# TPC-H Data Warehouse

> Modern data warehouse pipeline with medallion architecture using TPC-H benchmark dataset

## Architecture
### Pipeline Overview
![Architecture](images/architecture.jpg)

### Data Warehouse Schema
![Data Warehouse Schema](images/dw_architecture.png)


## Quick Start

```bash
# Clone and setup
git clone git@github.com:hungfnguyen/tcph-data-warehouse.git
cd tcph-data-warehouse
cp .env.example .env

# Start infrastructure
docker-compose up -d

# Run pipeline
make run-pipeline
```

## Features

- **ELT Pipeline**: Snowflake TPC-H → Data Lake → Data Warehouse
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (analytics)
- **Automated Orchestration**: Airflow DAGs with error handling
- **Data Quality**: Built-in validation and monitoring
- **Business Intelligence**: Interactive dashboards and KPIs

## Components

| Component | Purpose | URL |
|-----------|---------|-----|
| Airflow | Orchestration | http://localhost:8080 |
| MinIO | Object Storage | http://localhost:9001 |
| PostgreSQL | Data Warehouse | localhost:5432 |
| Metabase | Analytics | http://localhost:3000 |

## Key Metrics

- **8 source tables** → **4 fact tables + 5 dimensions**
- **6M+ records** processed through medallion layers
- **Sub-second query performance** on star schema
- **99%+ data quality** score with automated validation

## Project Goals

Learning modern data engineering practices including:
- Cloud data extraction and processing
- ELT vs ETL methodologies  
- Dimensional modeling and star schemas
- Data quality and pipeline monitoring
- Business intelligence and analytics

---

## Authors

- **Hung Nguyen** – [@hungfnguyen](https://github.com/hungfnguyen)
