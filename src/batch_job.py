import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.transformations import transform_wikimedia_data  # Import the shared function


class WikimediaBatchJob:
    """
    Orchestrates the Spark batch job from S3 JSON files to an Iceberg table.
    """

    def __init__(self, input_path: str, output_table: str):
        """Initializes the Spark session and job parameters."""
        self.spark = SparkSession.builder.appName("WikimediaBatch").getOrCreate()
        self.input_path = input_path
        self.output_table = output_table

    def run(self):
        """Executes the batch job."""
        print(f"Reading raw JSON data from: {self.input_path}")

        # Read raw multiline JSON files as text. This ensures each JSON object
        # is a single row, which we then place into a 'data' column to match
        # the input format expected by our transformation function.
        raw_df = self.spark.read.text(self.input_path) \
            .select(col("value").alias("data"))

        # Apply the centralized, testable, functional transformation
        processed_df = transform_wikimedia_data(raw_df)

        # Using .cache() can be beneficial here if the count is expensive
        # processed_df.cache()
        record_count = processed_df.count()
        print(f"Writing {record_count} records to Iceberg table: {self.output_table}")

        if record_count > 0:
            # Append the transformed data to the target Iceberg table
            processed_df.write \
                .format("iceberg") \
                .mode("append") \
                .save(self.output_table)
        else:
            print("No new records to write. Skipping.")

        print("Batch job completed successfully.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: batch_job.py <s3_input_path> <iceberg_table_name>")
        sys.exit(-1)

    s3_input_path = sys.argv[1]
    iceberg_table = sys.argv[2]

    job = WikimediaBatchJob(input_path=s3_input_path, output_table=iceberg_table)
    job.run()