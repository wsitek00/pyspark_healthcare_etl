from src.utils.spark_session import get_spark_session
from src.jobs.extraction_job import load_data
from src.transformations.symptoms import transform_symptoms_to_array
import os

def main():
    spark = get_spark_session("Healthcare_ETL_Main")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(current_dir, "data", "raw", "Healthcare.csv")
    
    try:
        print(f"--- Starting ETL Process ---")
        
        # 1. EXTRACT
        print(f"1. Extracting data...")
        raw_df = load_data(spark, input_path)
        
        # 2. TRANSFORM
        print(f"2. Transforming data (Parsing Symptoms)...")
        processed_df = transform_symptoms_to_array(raw_df)
        
        # 3. SHOW RESULTS
        print("Data Preview (After Transformation):")
        processed_df.select("Patient_ID", "Symptoms", "Symptoms_Array", "Actual_Symptom_Count").show(5, truncate=False)
        
        # QA Check (Quality Assurance)
        print("Data Quality Check (Sample discrepancy):")
        processed_df.filter("Symptom_Count != Actual_Symptom_Count").show()
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        
    finally:
        spark.stop()
        print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()