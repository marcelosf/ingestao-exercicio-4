import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def session():
    return SparkSession.builder.getOrCreate()