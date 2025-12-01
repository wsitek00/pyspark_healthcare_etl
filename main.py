from src.utils.spark_session import get_spark_session
from src.jobs.extraction_job import load_data
from src.transformations.symptoms import transform_symptoms_to_array
from src.jobs.analysis_job import analyze_symptom_frequency, analyze_disease_distribution_sql
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
        print(f"2. Transforming data...")
        processed_df = transform_symptoms_to_array(raw_df)
        
        # 3. ANALYSIS (Insights)
        print(f"3. Running Analysis...")
        
        print("\n--- TOP 5 MOST COMMON SYMPTOMS (PySpark Explode) ---")
        symptoms_df = analyze_symptom_frequency(processed_df)
        symptoms_df.show(5, truncate=False)
        
        print("\n--- TOP 10 DISEASES & AVG AGE (Spark SQL) ---")
        diseases_df = analyze_disease_distribution_sql(spark, processed_df)
        diseases_df.show(10, truncate=False)
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        
    finally:
        spark.stop()
        print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()