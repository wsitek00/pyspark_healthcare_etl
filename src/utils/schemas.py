from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def get_healthcare_schema() -> StructType:
    """
    Defines the explixit schema for the Healthcare dataset.
    Using explixit schema avoids the overhead of schema inference (inferSchema).
    """
    return StructType([
        StructField("Patient_ID", IntegerType(), True),
        StructField("Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("Symptoms", StringType(), True),
        StructField("Symptom_Count", IntegerType(), True),
        StructField("Disease", StringType(), True),
    ])
    