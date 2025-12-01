from pyspark.sql import DataFrame
import pandas as pd
import os

def save_data(df: DataFrame, output_path: str, format: str = "parquet"):
    """
    Saves the DataFrame using Pandas to bypass Hadoop/Winutils issues on Windows.
    
    Args:
        df (DataFrame): Spark DataFrame to save.
        output_path (str): Destination folder/file path.
        format (str): File format (parquet, csv).
    """
    print(f"Saving data to {output_path} (Format: {format})...")
    
    # Conversion: Spark DataFrame -> Pandas DataFrame
    # NOTE: This is a workaround for local Windows development to avoid 
    # Hadoop/Winutils configuration issues.
    # In a production environment (Linux/Cloud), we would use: df.write.save()
    try:
        pandas_df = df.toPandas()
        
        # Create output directory if it does not exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        if format == "parquet":
            # 'pyarrow' engine is required for parquet export
            pandas_df.to_parquet(f"{output_path}.parquet", engine='pyarrow', index=False)
        elif format == "csv":
            pandas_df.to_csv(f"{output_path}.csv", index=False)
        else:
            print(f"Format '{format}' is not supported in this local bypass mode.")
            return

        print("Data saved successfully (Local Bypass).")
        
    except Exception as e:
        print(f"Error saving data: {e}")