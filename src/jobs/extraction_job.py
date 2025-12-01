from pyspark.sql import SparkSession, DataFrame
from src.utils.schemas import get_healthcare_schema
import os

def load_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Loads healtchcare data from CSV file using a predefined schema.
    
    Args:
        spark (SparkSession): Active Spark Session.
        file_path (str): Path to the CSV file.
    
    Return:
        DataFrame: Loaded data.
    """
    schema = get_healthcare_schema()
    # Check if file exists to provide a clear error message
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"The file '{file_path}' does not exist. Please check the path.")

    # Read CSV with header and enforced schema
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .schema(schema) \
        .load(file_path)
        
    return df
    