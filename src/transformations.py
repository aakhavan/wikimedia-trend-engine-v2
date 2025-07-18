from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, from_unixtime
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

def transform_wikimedia_data(raw_df: DataFrame) -> DataFrame:
    """
    Applies schema, flattens, and cleans raw Wikimedia event data.
    This function is "pure" and easily testable. It takes a DataFrame
    and returns a transformed DataFrame without any side effects.
    """
    # The input DataFrame is expected to have a single column "data" containing the JSON string.
    parsed_df = raw_df.select(from_json(col("data"), SCHEMA).alias("wikimedia"))

    # Select and flatten the nested structure into a clean, tabular format.
    return parsed_df.select(
        col("wikimedia.id").alias("event_id"),
        col("wikimedia.type").alias("event_type"),
        col("wikimedia.title").alias("page_title"),
        col("wikimedia.comment"),
        from_unixtime(col("wikimedia.timestamp")).alias("event_timestamp"),
        col("wikimedia.user"),
        col("wikimedia.bot"),
        col("wikimedia.meta.domain").alias("domain"),
        col("wikimedia.server_name"),
        col("wikimedia.length.new").alias("new_length"),
        col("wikimedia.length.old").alias("old_length")
    )