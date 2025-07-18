from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.transformations import transform_wikimedia_data # Keep functional transform


class WikimediaStreamingJob:
    """
    Orchestrates the Spark Structured Streaming job from Kinesis to Iceberg.
    """

    def __init__(self):
        """Initializes the Spark session and retrieves job configurations."""
        self.spark = SparkSession.builder.appName("WikimediaStreaming").getOrCreate()
        self.conf = self.spark.sparkContext.getConf()
        self.kinesis_stream_name = self.conf.get("spark.app.kinesisStreamName")
        self.aws_region = self.conf.get("spark.app.awsRegion")
        self.iceberg_db_name = self.conf.get("spark.app.icebergDbName")
        self.s3_bucket = self.conf.get("spark.app.s3BucketName")

    def _read_from_kinesis(self):
        """Creates a streaming DataFrame that reads from the Kinesis stream."""
        return self.spark.readStream \
            .format("kinesis") \
            .option("streamName", self.kinesis_stream_name) \
            .option("endpointUrl", f"https://kinesis.{self.aws_region}.amazonaws.com") \
            .option("startingPosition", "latest") \
            .load()

    def run(self):
        """Executes the streaming job."""
        kinesis_df = self._read_from_kinesis()

        # Cast the binary 'data' column to a string for transformation
        string_df = kinesis_df.select(col("data").cast("string").alias("data"))

        # Apply the centralized, testable, functional transformation
        processed_df = transform_wikimedia_data(string_df)

        # Define the output Iceberg table and checkpoint location
        output_table = f"glue_catalog.{self.iceberg_db_name}.recent_changes_stream"
        checkpoint_location = f"s3://{self.s3_bucket}/checkpoints/streaming/"

        # Write the stream to the Iceberg table
        query = processed_df.writeStream \
            .format("iceberg") \
            .outputMode("append") \
            .trigger(processingTime='60 seconds') \
            .option("path", output_table) \
            .option("checkpointLocation", checkpoint_location) \
            .start()

        query.awaitTermination()


if __name__ == "__main__":
    job = WikimediaStreamingJob()
    job.run()