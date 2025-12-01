from src.transformations.clean_patient_data import filter_valid_patients
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def test_filter_valid_patients(spark):
    # 1. ARRANGE (Prepare test data)
    data = [
        ("John", 25),   # Valid
        ("Anna", -5),   # Invalid (negative)
        ("Tom", 150),   # Invalid (too old)
        ("Eve", 0)      # Valid (boundary)
    ]
    
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])
    
    input_df = spark.createDataFrame(data, schema)

    # 2. ACT (Execute function)
    output_df = filter_valid_patients(input_df)

    # 3. ASSERT (Verify results)
    results = output_df.collect()
    
    # We expect only 2 records (John and Eve)
    assert len(results) == 2
    
    names = [row.name for row in results]
    assert "John" in names
    assert "Eve" in names
    assert "Anna" not in names