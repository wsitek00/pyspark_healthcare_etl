# Healthcare ETL Pipeline with PySpark

## Project Overview
This project implements a scalable ETL (Extract, Transform, Load) pipeline for processing healthcare data. It focuses on data quality, transformation logic separation, and software engineering best practices (SDLC).

## Tech Stack
* **Language:** Python 3.10+
* **Processing Engine:** Apache Spark (PySpark)
* **Testing:** Pytest (Unit Testing)
* **CI/CD:** GitHub Actions (ready)
* **Code Quality:** Black, Flake8

## Project Structure
```text
pyspark_healthcare_etl/
├── config/             # Configuration files
├── src/                # Source code
│   ├── jobs/           # ETL Job orchestration
│   ├── transformations/# Pure transformation logic(business rules)
│   └── utils/          # Helper functions (SparkSession factory)
├── tests/              # Unit tests
└── requirements.txt    # Project dependencies