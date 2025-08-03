from __future__ import annotations
import pendulum
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

# Use Airflow Variables for configuration. This is the standard, secure way.
# You must set these in the Airflow UI (Admin -> Variables).
EMR_SERVERLESS_APP_ID = "{{ var.value.emr_serverless_app_id }}"
EMR_EXECUTION_ROLE_ARN = "{{ var.value.emr_execution_role_arn }}"
S3_BUCKET_NAME = "{{ var.value.s3_bucket_name }}"
GLUE_DATABASE_NAME = "{{ var.value.glue_database_name }}"

# Define S3 path for the script
S3_SCRIPT_PATH = f"s3://{S3_BUCKET_NAME}/spark_scripts/batch_job.py"

with DAG(
    dag_id="wikimedia_daily_batch_aggregation",
    # Using pendulum is a best practice for timezone-aware scheduling
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    # Using a standard cron preset is clear and easy to read
    schedule="@daily",
    tags=["wikimedia", "batch", "emr"],
    doc_md="""
    ### Wikimedia Daily Batch Aggregation DAG
    Orchestrates a daily batch job on EMR Serverless.
    Dependencies are downloaded automatically by Spark.
    """,
) as dag:
    start_batch_job = EmrServerlessStartJobOperator(
        task_id="start_emr_batch_job",
        application_id=EMR_SERVERLESS_APP_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_SCRIPT_PATH,
                # Pass dynamic values to the Spark job via --conf
                "sparkSubmitParameters": f"--conf spark.app.icebergDbName={GLUE_DATABASE_NAME}",
            }
        },
        # Use configuration_overrides for static environment setup
        configuration_overrides={
            "applicationConfiguration": [
                {
                    "classification": "spark-defaults",
                    "properties": {
                        # This line makes the DAG self-contained and reliable
                        "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3",
                        # Standard Iceberg configurations
                        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.SparkSqlExtensions",
                        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
                        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                        "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                        "spark.sql.catalog.glue_catalog.warehouse": f"s3://{S3_BUCKET_NAME}/data/"
                    },
                }
            ],
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{S3_BUCKET_NAME}/logs/emr-serverless/batch/"}
            },
        },
        wait_for_completion=True,
    )