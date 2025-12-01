import pytest
import os
import sys
from src.utils.spark_session import get_spark_session

@pytest.fixture(scope="session")
def spark():
    """
    Creates a single SparkSession shared across all tests in the session.
    The session is stopped after all tests are finished.
    """
    # --- WINDOWS WORKAROUND ---
    # Sets a temporary directory for Hadoop/Hive to avoid winutils errors on Windows
    if sys.platform.startswith('win'):
        os.makedirs("C:\\tmp\\hive", exist_ok=True)
        os.environ['HADOOP_HOME'] = "C:\\tmp\\hive"
    # --------------------------

    spark_session = get_spark_session("Test_Session")
    yield spark_session
    spark_session.stop()