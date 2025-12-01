from pyspark.sql import SparkSession
import os
import sys

def get_spark_session(app_name: str = "Healthcare_ETL_Pipeline") -> SparkSession:
    """
    Creates or retrieves an existing SparkSession.
    Configuration is optimized for local development.
    
    Args:
        app_name (str): Name of the application visible in Spark UI.

    Returns:
        SparkSession: The active Spark session object.
    """
    try:
        builder = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.sql.shuffle.partitions", "2")  # Optimization for small local data
            
        # Optional: Delta Lake configuration (uncomment if needed)
        # builder = builder \
        #     .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
        #     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        #     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        spark = builder.getOrCreate()
        
        # Set logging level to WARN to reduce console noise
        spark.sparkContext.setLogLevel("WARN")
        
        return spark

    except Exception as e:
        print(f"Error creating Spark Session: {e}")
        raise

if __name__ == "__main__":
    spark = get_spark_session("Test_Session")
    print(f"Session created successfully! Spark version: {spark.version}")
    spark.stop()