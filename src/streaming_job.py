# File: src/streaming_job.py
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

# The transformation logic is pure and can be reused without changes.
from transformations import transform_wikimedia_data, SCHEMA


class WikimediaStreamingJob:
    """
    Orchestrates the Spark DStreams job from Kinesis to Iceberg using the older ASL connector.
    """

    def __init__(self):
        """Initializes the Spark and Streaming Contexts."""
        self.spark = SparkSession.builder.appName("WikimediaStreamingDStream").getOrCreate()
        self.sc = self.spark.sparkContext
        # Create a StreamingContext with a 60-second batch interval
        self.ssc = StreamingContext(self.sc, 60)

        # Get config from Spark conf passed by manage_jobs.py
        self.conf = self.sc.getConf()
        self.kinesis_stream_name = self.conf.get("spark.app.kinesisStreamName")
        self.aws_region = self.conf.get("spark.app.awsRegion")
        self.iceberg_db_name = self.conf.get("spark.app.icebergDbName")

    def _process_rdd(self, time, rdd):
        """
        This function is applied to every micro-batch (RDD) of data from the Kinesis stream.
        Note: The function must accept both 'time' and 'rdd' arguments from foreachRDD.
        """
        if rdd.isEmpty():
            print(f"--- Batch for time: {time} was empty. Skipping. ---")
            return

        print(f"--- Processing batch for time: {time} ---")
        # The RDD from Kinesis contains raw bytes. We must decode them to strings.
        string_rdd = rdd.map(lambda bytes_record: bytes_record.decode('utf-8'))

        # Let Spark parse the RDD of JSON strings into a DataFrame using a predefined schema
        # This is more efficient and robust than creating a single-column DataFrame first.
        parsed_df = self.spark.read.schema(SCHEMA).json(string_rdd)

        # Check if the DataFrame is empty after parsing (e.g., all JSON was malformed)
        if parsed_df.isEmpty():
            print("--- DataFrame was empty after JSON parsing. Skipping. ---")
            return

        # Apply the transformation logic
        processed_df = transform_wikimedia_data(parsed_df)

        # Check if the transformation produced any records before writing
        if processed_df.isEmpty():
            print("--- Batch was empty after transformation, skipping write. ---")
            return

        record_count = processed_df.count()
        print(f"Transformed {record_count} records in this batch.")
        processed_df.show(5, truncate=False)

        # Write the transformed data for this batch to the Iceberg table
        output_table = f"glue_catalog.{self.iceberg_db_name}.recent_changes_stream"
        print(f"Writing batch to Iceberg table: {output_table}")
        processed_df.write \
            .format("iceberg") \
            .mode("append") \
            .save(output_table)
        print("--- Batch write complete ---")

    def run(self):
        """Executes the DStreams-based streaming job."""
        endpoint_url = f"https://kinesis.{self.aws_region}.amazonaws.com"
        # This app name is used by the Kinesis Client Library to create a DynamoDB table
        # for managing its state (e.g., which shards it's reading from).
        kinesis_app_name = "WikimediaStreamingDStream"
        print(f"Creating Kinesis DStream for stream '{self.kinesis_stream_name}' with consumer app name '{kinesis_app_name}'")

        # Use KinesisUtils to create the DStream. This is the core of the older API.
        kinesis_stream = KinesisUtils.createStream(
            self.ssc,
            kinesis_app_name,           # Unique Kinesis consumer application name
            self.kinesis_stream_name,   # The actual Kinesis stream to read from
            endpoint_url,
            self.aws_region,
            InitialPositionInStream.LATEST,
            60,  # Checkpoint interval, should match the batch interval
        )

        # Tell the stream to apply our processing function to each RDD
        kinesis_stream.foreachRDD(self._process_rdd)

        # Start the streaming context and wait for it to be stopped
        print("Starting streaming context...")
        self.ssc.start()
        self.ssc.awaitTermination()


if __name__ == "__main__":
    job = WikimediaStreamingJob()
    job.run()