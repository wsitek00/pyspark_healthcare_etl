from src.utils.spark_session import get_spark_session
from src.jobs.extraction_job import load_data
import os

def main():
    # 1. Initialize Spark Session
    spark = get_spark_session("Healthcare_ETL_Main")
    
    # 2. Define paths (best practice: use relative paths)
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(current_dir, "data", "raw", "Healthcare.csv")
    
    try:
        print(f"--- Starting ETL Process ---")
        
        # 3. Extract (Load Data)
        print(f"Loading data from: {input_path}")
        raw_df = load_data(spark, input_path)
        
        # 4. Basic Data Exploration (Show results)
        print("Schema:")
        raw_df.printSchema()
        
        print("Data Preview (Top 5 rows):")
        raw_df.show(5, truncate=False)
        
        print(f"Total records: {raw_df.count()}")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        
    finally:
        # 5. Clean up
        spark.stop()
        print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()