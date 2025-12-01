from src.utils.spark_session import get_spark_session
from src.jobs.extraction_job import load_data
from src.transformations.symptoms import transform_symptoms_to_array
from src.jobs.analysis_job import analyze_symptom_frequency, analyze_disease_distribution_sql
from src.jobs.load_job import save_data
import os
import sys

def main():
    spark = get_spark_session("Healthcare_ETL_Main")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define Paths
    input_path = os.path.join(current_dir, "data", "raw", "Healthcare.csv")
    
    # NOTE: For Pandas saving, we provide the file path prefix. 
    # The extension (.parquet/.csv) will be added inside the save function.
    output_path = os.path.join(current_dir, "data", "processed", "healthcare_analysis")
    report_path = os.path.join(current_dir, "data", "processed", "top_diseases_report")

    try:
        print(f"--- Starting ETL Process ---")
        
        # 1. EXTRACT
        print(f"1. Extracting data...")
        raw_df = load_data(spark, input_path)
        
        # 2. TRANSFORM
        print(f"2. Transforming data...")
        processed_df = transform_symptoms_to_array(raw_df)
        
        # 3. ANALYSIS
        print(f"3. Running Analysis...")
        symptoms_df = analyze_symptom_frequency(processed_df)
        diseases_df = analyze_disease_distribution_sql(spark, processed_df)
        
        print("\n--- TOP 5 SYMPTOMS ---")
        symptoms_df.show(5)
        
        # 4. LOAD (Saving Results via Pandas Bypass)
        print(f"4. Loading (Saving) data...")
        
        # Save main analysis as Parquet (Columnar storage)
        save_data(processed_df, output_path, format="parquet")
        
        # Save disease report as CSV (Business readable)
        save_data(diseases_df, report_path, format="csv")
        
    except Exception as e:
        print(f"Pipeline failed: {e}")
        # Print full traceback for debugging purposes
        import traceback
        traceback.print_exc()
        
    finally:
        spark.stop()
        print("--- Pipeline Finished ---")

if __name__ == "__main__":
    main()