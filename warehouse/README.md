# Wildfire Prediction - Data Warehouse

## 1. Overview

Data Warehouse cho hệ thống dự báo cháy rừng tại Mỹ (2014-2025).

Technology:
- Database: MySQL 8.4
- Data Processing: PySpark (integration pending)
- Architecture: Star Schema
- Data Source: US Wildfire Dataset

Data Flow:
Kaggle -> MinIO -> PySpark ETL -> MySQL DWH -> EDA / ML / Power BI

## 2. Project Structure

warehouse/
  sql/
    schema.sql
    test_data.sql
    queries.sql
    quality_checks.sql
    views.sql
  scripts/
  data/
  README.md

## 3. Database Schema

Database: wildfire_dw

### dim_date
- date_key (PK)
- full_date
- year_num
- month_num
- day_num

### dim_location
- location_key (PK)
- latitude
- longitude

### fact_wildfire
- fact_id (PK)
- date_key (FK)
- location_key (FK)
- observed_at
- wildfire (0/1)
- 15 environmental features

Relationships:
- fact_wildfire.date_key -> dim_date.date_key
- fact_wildfire.location_key -> dim_location.location_key

Fact grain: one row per source observation.

## 4. Local Setup

Requirements:
- MySQL Server 8.4
- VS Code
- MySQL CLI

Connect to MySQL:

mysql -u root -p

If mysql is not in PATH, use the full path to mysql.exe.

Create database:

CREATE DATABASE IF NOT EXISTS wildfire_dw;
USE wildfire_dw;

Run SQL files using the MySQL CLI:

SOURCE <absolute-path>/sql/schema.sql;
SOURCE <absolute-path>/sql/test_data.sql;
SOURCE <absolute-path>/sql/queries.sql;
SOURCE <absolute-path>/sql/quality_checks.sql;
SOURCE <absolute-path>/sql/views.sql;

Notes:
- Run test_data.sql only for local testing.
- Test data is synthetic and must be removed before production loading.
- Running test_data.sql repeatedly may cause duplicate-key errors.
- views.sql must exist before executing it.
- Replace <absolute-path> with your actual local project path.

## 5. Current Progress

Completed:
- MySQL database initialization
- Initial Star Schema design
- Dimension and Fact table creation
- Sample data insertion
- JOIN testing
- Analytical SQL queries
- Basic data quality checks

Pending:
- Verify schema against the real dataset
- Confirm PySpark output data contract
- Implement bulk data loading
- Load and validate real data
- Performance testing
- Power BI integration

## 6. Data Contract (Provisional)

Expected fields:

latitude, longitude, datetime, Wildfire,
pr, rmax, rmin, sph, srad, tmmn, tmmx,
vs, bi, fm100, fm1000, erc, etr, pet, vpd

Requirements:
- Datetime must be convertible to MySQL DATETIME.
- Wildfire label must be binary (0 or 1).
- Coordinates must be valid.
- Date dimension key uses YYYYMMDD.
- Missing values and duplicates require validation.

Important: This contract is provisional until the real
dataset and PySpark output schema are confirmed.

## 7. Team Collaboration

Shared branch: feature/data-warehouse

Suggested responsibilities:

Member A:
- Database schema
- Dimension and Fact tables
- Analytical queries
- Data quality checks

Member B:
- PySpark to MySQL integration
- Data loading scripts
- Bulk loading and performance tests
- Data validation

Both members:
- Review schema changes
- Maintain README
- Test integration
- Coordinate before editing the same file

## 8. Git Workflow

Before starting work:

git switch feature/data-warehouse
git pull --rebase origin feature/data-warehouse

After completing work:

git status
git add .
git commit -m "describe your changes"
git pull --rebase origin feature/data-warehouse
git push origin feature/data-warehouse

Rules:
- Do not commit database passwords.
- Do not commit .env files containing secrets.
- Do not commit large raw datasets.
- Communicate before modifying shared SQL files.
- Commit small, logically separated changes.
- Run relevant SQL checks before pushing.

## 9. Next Milestone

Integrate PySpark ETL output with MySQL Data Warehouse
and validate the loaded wildfire dataset.
