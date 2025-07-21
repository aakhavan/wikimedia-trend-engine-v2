import argparse
import logging
import boto3
import yaml
from pathlib import Path
import time
from botocore.exceptions import ClientError
import subprocess
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_terraform_outputs() -> dict:
    """
    Runs 'terraform output -json' and returns the parsed outputs.
    This makes the script self-sufficient and always uses live data.
    """
    logging.info("Fetching live infrastructure details from Terraform...")
    try:
        result = subprocess.run(
            ["terraform", "output", "-json"],
            capture_output=True, text=True, check=True, cwd="./terraform"
        )
        outputs = json.loads(result.stdout)
        # The output values are nested, so we extract them
        return {key: value['value'] for key, value in outputs.items()}
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to get Terraform outputs: {e}")
        logging.error("Please ensure you are in the project root and Terraform has been applied.")
        raise


def get_base_config() -> dict:
    """Loads the base configuration template from the local file."""
    try:
        config_path = Path(__file__).parent / 'config/config.yml'
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError) as e:
        logging.error(f"Could not load or parse base config/config.yml. Error: {e}")
        raise


def start_streaming_job(emr_client, config: dict):
    """
    Constructs and starts the EMR Serverless streaming job using the live config.
    """
    aws_config = config['aws']
    s3_bucket = aws_config['s3_bucket']
    emr_app_id = aws_config['emr_serverless_app_id']
    emr_role_arn = aws_config['emr_execution_role_arn']

    job_name = f"wikimedia-streaming-job-{int(time.time())}"
    logging.info(f"Starting streaming job with name: {job_name}")

    entry_point = f"s3://{s3_bucket}/spark_scripts/streaming_job.py"

    # The streaming job gets its specific parameters from the config file.
    # We pass these as --conf arguments to the Spark job.
    spark_submit_params = (
        f"--conf spark.app.kinesisStreamName={aws_config['kinesis_stream_name']} "
        f"--conf spark.app.awsRegion={aws_config['region']} "
        f"--conf spark.app.icebergDbName={config['airflow']['batch_job']['iceberg_db_name']} "
        f"--conf spark.app.s3BucketName={s3_bucket}"
    )

    try:
        response = emr_client.start_job_run(
            applicationId=emr_app_id,
            executionRoleArn=emr_role_arn,
            name=job_name,
            jobDriver={
                'sparkSubmit': {
                    'entryPoint': entry_point,
                    'sparkSubmitParameters': spark_submit_params
                }
            },
            # --- THIS IS THE FIX ---
            # Provide job-specific configurations, including required packages.
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{s3_bucket}/logs/emr-serverless/streaming/"}
                },
                "applicationConfiguration": [
                    {
                        "classification": "spark-defaults",
                        "properties": {
                            "spark.jars.packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,org.apache.spark:spark-sql-kinesis-assembly_2.12:3.4.1",
                            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.SparkSqlExtensions",
                            "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
                            "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
                            "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
                            "spark.sql.catalog.glue_catalog.warehouse": f"s3://{s3_bucket}/data/"
                        }
                    }
                ]
            }
        )
        job_run_id = response['jobRunId']
        logging.info(f"✅ Successfully started streaming job. Job Run ID: {job_run_id}")
        logging.info("Monitor its status in the AWS EMR Serverless console.")
        return job_run_id
    except ClientError as e:
        logging.error(f"❌ Failed to start streaming job: {e}")
        raise


def main():
    """
    Main function to parse arguments and dispatch commands.
    """
    parser = argparse.ArgumentParser(description="Manage EMR Serverless Jobs for the Wikimedia Project")
    subparsers = parser.add_subparsers(dest="command", required=True, help="Available commands")

    # --- 'start' command ---
    start_parser = subparsers.add_parser("start", help="Start a new job run")
    start_parser.add_argument("job_type", choices=["streaming"], help="The type of job to start.")

    args = parser.parse_args()

    # --- Build the live configuration in memory ---
    try:
        tf_outputs = get_terraform_outputs()
        config = get_base_config()

        # Populate the config with live values from Terraform
        config['aws']['s3_bucket'] = tf_outputs.get('data_lake_s3_bucket_name')
        config['aws']['emr_serverless_app_id'] = tf_outputs.get('emr_serverless_app_id')
        config['aws']['emr_execution_role_arn'] = tf_outputs.get('emr_execution_role_arn')
        # --- THIS IS THE FIX ---
        # Populate the Glue DB name from the new Terraform output.
        config['airflow']['batch_job']['iceberg_db_name'] = tf_outputs.get('glue_database_name')

        # Simple validation
        if not all([
            config['aws']['s3_bucket'],
            config['aws']['emr_serverless_app_id'],
            config['aws']['emr_execution_role_arn'],
            config['airflow']['batch_job']['iceberg_db_name']
        ]):
            raise ValueError("Could not populate all required config values from Terraform outputs.")

    except Exception as e:
        logging.error(f"Failed to build job configuration: {e}")
        exit(1)

    emr_client = boto3.client("emr-serverless", region_name=config['aws']['region'])

    if args.command == "start":
        if args.job_type == "streaming":
            start_streaming_job(emr_client, config)


if __name__ == "__main__":
    main()