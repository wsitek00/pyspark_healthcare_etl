from pyspark.sql import DataFrame
from pyspark.sql.functions import col

def filter_valid_patients(df: DataFrame) -> DataFrame:
    """
    Filters patients, keeping only those with valid age (0-120).
    
    Args:
        df (DataFrame): Input DataFrame containing an 'age' column.
        
    Returns:
        DataFrame: Filtered DataFrame.
    
    Raises:
        ValueError: If 'age' column is missing.
    """
    if "age" not in df.columns:
        raise ValueError("DataFrame must contain 'age' column")
        
    return df.filter((col("age") >= 0) & (col("age") <= 120))