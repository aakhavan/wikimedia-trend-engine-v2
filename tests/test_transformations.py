import pytest
from pyspark.sql import SparkSession, Row
from src.transformations import transform_wikimedia_data

@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for the test suite, configured for UTC."""
    return SparkSession.builder \
        .master("local[2]") \
        .appName("WikimediaTest") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()

def test_transform_wikimedia_data(spark):
    """
    Tests the core Spark transformation logic.
    """
    # 1. Create sample input data mimicking raw Kinesis records
    input_json = """
    {
        "meta":{"domain":"test.wikipedia.org"},
        "id":9876, "type":"new", "title":"Test Page", "comment":"Creating page",
        "timestamp":1672531200, "user":"TestUser", "bot":false,
        "server_name":"test.server.org", "length":{"new":150, "old":0}
    }
    """
    input_df = spark.createDataFrame([Row(data=input_json)])

    # 2. Apply the transformation function
    result_df = transform_wikimedia_data(input_df)
    result_data = result_df.collect()[0] # Get the first (and only) row

    # 3. Assert the output is correct
    assert result_df.count() == 1
    assert result_data["event_id"] == 9876
    assert result_data["domain"] == "test.wikipedia.org"
    assert result_data["user"] == "TestUser"
    assert result_data["bot"] is False
    assert result_data["new_length"] == 150
    # Check timestamp conversion (1672531200 -> 2023-01-01 00:00:00)
    assert str(result_data["event_timestamp"]) == "2023-01-01 00:00:00"