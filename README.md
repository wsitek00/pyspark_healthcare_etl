# Healthcare Data ETL Pipeline 🏥

## Project Overview
This project implements a scalable **ETL (Extract, Transform, Load)** pipeline using **Apache Spark (PySpark)** to analyze healthcare data. 

The goal was to process a raw dataset of 25,000 patients, clean the data, parse unstructured text (symptoms), and derive business insights such as the most common diseases and symptoms distribution.

## 🏗 Architecture
The pipeline follows modern Data Engineering best practices:
1.  **Extract:** Ingests raw CSV data with explicit schema enforcement (avoiding `inferSchema` overhead).
2.  **Transform:**
    * Parses complex string columns (symptoms list) into arrays using PySpark functions.
    * Performs Data Quality (DQ) checks to ensure data consistency.
3.  **Analyze:**
    * Uses **PySpark DataFrame API** for array operations (`explode`).
    * Uses **Spark SQL** for complex aggregations (mixing SQL with Python).
4.  **Load:** Saves processed data in **Parquet** format (columnar storage) and business reports in **CSV**.

## 🛠 Tech Stack
* **Language:** Python 3.10+
* **Processing Engine:** Apache Spark 3.5.0 (PySpark)
* **Testing:** Pytest (Unit Tests with shared SparkSession fixture)
* **CI/CD:** GitHub Actions (Automated testing on push)
* **Version Control:** Git & GitHub
* **Local Dev Tools:** Pandas & PyArrow (used for local storage optimization)

## 📂 Project Structure
```text
pyspark_healthcare_etl/
├── .github/workflows/  # CI/CD pipelines
├── config/             # Configuration files
├── data/               # Local data storage (ignored by Git)
│   ├── raw/            # Raw CSV input
│   └── processed/      # Output Parquet/CSV files
├── src/                # Source Code
│   ├── jobs/           # ETL Logic (Extract, Load, Analysis)
│   ├── transformations/# Pure business logic (unit-testable)
│   └── utils/          # SparkSession Factory & Schemas
├── tests/              # Unit Tests
├── main.py             # Pipeline Entry Point
└── requirements.txt    # Dependencies
