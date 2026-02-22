# 📡 Automated Cloud ETL Pipeline for Traffic Violations & Weather Data

This project implements a fully automated, serverless ETL pipeline on AWS that ingests daily public traffic violation records and weather data into a relational MySQL database hosted on Amazon RDS.

The system is designed not for immediate analytics, but to create a secure, continuously updated data storage layer that enables accurate downstream analysis of environmental factors affecting traffic enforcement activity.

## Architecture Overview

The pipeline integrates:

- **AWS Lambda** for daily incremental ingestion
- **Amazon RDS (MySQL)** for persistent relational storage
- **AWS EventBridge** for scheduled orchestration
- **AWS Secrets Manager** for secure credential injection
- **Python-based ETL logic** for idempotent data insertion
- **Join-ready relational schema** for future analytics

Daily ingestion pipelines are deployed as Lambda functions and automatically triggered via EventBridge schedules.

Database credentials are never stored in code and are retrieved securely at runtime using AWS Secrets Manager.

## Data Sources

### 🚗 Moving Violations (District of Columbia Open Data)

- **Source:** https://opendata.dc.gov
- **Access:** ArcGIS REST API
- **Update Frequency:** Daily
- **Primary Key:** violation_id
- **Fields:** issuing agency, violation type, fine amount, coordinates, accident indicator

### 🌦 Weather Data (VisualCrossing API)

- **Source:** https://www.visualcrossing.com
- **Access:** Daily API ingestion
- **Primary Key:** weather_date
- **Fields:** precipitation, temperature, humidity, wind speed, rainfall indicator

## Relational Database Design

The database schema is structured to support cross-domain analysis by linking traffic violations with daily weather conditions using a shared date dimension.

**Example analytical query:**

```sql
SELECT COUNT(*)
FROM violations v
JOIN weather_daily w
  ON v.violation_date = w.weather_date
WHERE w.is_rain = 1;
```

This allows investigation of potential correlations between weather conditions and enforcement patterns.

## Security & Configuration

All database credentials and API keys are injected at runtime via:

- **AWS Secrets Manager**
- **Lambda environment variables**

No secrets are stored in the repository.

Local historical full-load scripts support optional `.env` configuration for initial database bootstrapping, while production ingestion uses IAM-based access.

## Pipeline Design

The system supports:

- **One-time historical full load** (local execution)
- **Automated daily incremental ingestion** (AWS Lambda)
- **Duplicate-safe insert logic** (idempotent writes)
- **Scheduled orchestration** via EventBridge

## Portfolio Relevance

This project demonstrates:

- **Serverless ingestion pipeline design**
- **Cloud-native credential management**
- **Incremental data ingestion strategy**
- **Relational schema for analytical workloads**
- **Production-style separation of secrets from code**