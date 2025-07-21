from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col


class WikimediaBatchJob:
    """
    A batch job that reads from the streaming Iceberg table, performs an
    aggregation, and writes the result to a new summary table.
    """

    def __init__(self):
        """Initializes the Spark session and retrieves job configurations."""
        self.spark = SparkSession.builder.appName("WikimediaBatchAggregation").getOrCreate()
        self.conf = self.spark.sparkContext.getConf()
        self.iceberg_db_name = self.conf.get("spark.app.icebergDbName")
        self.input_table = f"glue_catalog.{self.iceberg_db_name}.recent_changes_stream"
        self.output_table = f"glue_catalog.{self.iceberg_db_name}.domain_edit_counts"

    def run(self):
        """Executes the batch aggregation job."""
        print(f"Reading from input table: {self.input_table}")

        # Read the raw streaming data from the Iceberg table
        streaming_data_df = self.spark.read.table(self.input_table)

        # Perform a simple aggregation: count edits per domain
        # This is a classic batch operation on streaming data.
        domain_counts_df = streaming_data_df.groupBy("domain") \
            .agg(count("*").alias("edit_count")) \
            .orderBy(col("edit_count").desc())

        print(f"Writing aggregation results to: {self.output_table}")

        # Write the aggregated data to a new Iceberg table, overwriting it each time
        domain_counts_df.write \
            .format("iceberg") \
            .mode("overwrite") \
            .save(self.output_table)

        print("Batch job completed successfully.")


if __name__ == "__main__":
    job = WikimediaBatchJob()
    job.run()
