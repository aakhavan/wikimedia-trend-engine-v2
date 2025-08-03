from pyspark.sql import DataFrame
from pyspark.sql.functions import col, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

# The schema is now in a shared location, the single source of truth.
SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType(), True)
    ]), True),
    StructField("id", LongType(), True),
    StructField("type", StringType(), True),
    StructField("namespace", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("comment", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("user", StringType(), True),
    StructField("bot", BooleanType(), True),
    StructField("server_name", StringType(), True),
    StructField("length", StructType([
        StructField("new", IntegerType(), True),
        StructField("old", IntegerType(), True)
    ]), True)
])

def transform_wikimedia_data(parsed_df: DataFrame) -> DataFrame:
    """
    Flattens and cleans a DataFrame that has already been parsed from raw Wikimedia event JSON.
    This function is "pure" and easily testable. It takes a DataFrame
    and returns a transformed DataFrame without any side effects.
    """
    # The input DataFrame is now expected to be already parsed from JSON.

    # Select and flatten the nested structure into a clean, tabular format.
    return parsed_df.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        col("title").alias("page_title"),
        col("comment"),
        from_unixtime(col("timestamp")).alias("event_timestamp"),
        col("user"),
        col("bot"),
        col("meta.domain").alias("domain"),
        col("server_name"),
        col("length.new").alias("new_length"),
        col("length.old").alias("old_length")
    )