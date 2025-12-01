from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import explode, col, desc

def analyze_symptom_frequency(df: DataFrame) -> DataFrame:
    """
    Analyzes which symptoms are the most common across all patients.
    Uses the 'explode' function to flatten the array of symptoms.
    """
    # 1. Explode: Turn [a, b] into two rows: a, b
    df_exploded = df.withColumn("Symptom", explode(col("Symptoms_Array")))
    
    # 2. Group by Symptom and Count
    df_count = df_exploded.groupBy("Symptom") \
        .count() \
        .orderBy(desc("count"))
        
    return df_count

def analyze_disease_distribution_sql(spark: SparkSession, df: DataFrame) -> DataFrame:
    """
    Uses raw SQL to calculate disease statistics.
    Demonstrates ability to mix PySpark and SQL.
    """
    # 1. Register DataFrame as a temporary SQL view
    df.createOrReplaceTempView("patients_view")
    
    # 2. Execute SQL Query
    # We want top 10 diseases and average age of patients suffering from them
    query = """
        SELECT 
            Disease,
            COUNT(*) as Total_Cases,
            ROUND(AVG(Age), 1) as Avg_Patient_Age
        FROM patients_view
        GROUP BY Disease
        ORDER BY Total_Cases DESC
        LIMIT 10
    """
    
    return spark.sql(query)