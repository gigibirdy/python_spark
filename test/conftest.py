import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("talent path DE course").getOrCreate()
    return spark








