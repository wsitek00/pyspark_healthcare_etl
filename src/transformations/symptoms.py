from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, size

def transform_symptoms_to_array(df: DataFrame) -> DataFrame:
    """
    Transforms the 'Symptoms' string column into an array of strings.
    Also calculates the actual number of symptoms based on the array size
    to verify data consistency against 'Symptom_Count'.
    
    Args:
        df (DataFrame): Input DataFrame with 'Symptoms' column.
        
    Returns:
        DataFrame: DataFrame with new column 'Symptoms_Array' and 'Actual_Count'.
    """
    # 1. Split string by comma and space ", "
    # Input: "fever, back pain" -> Output: ["fever", "back pain"]
    df_transformed = df.withColumn("Symptoms_Array", split(col("Symptoms"), ", "))
    
    # 2. (Optional but good for QA) Calculate length of array
    df_transformed = df_transformed.withColumn("Actual_Symptom_Count", size(col("Symptoms_Array")))
    
    return df_transformed