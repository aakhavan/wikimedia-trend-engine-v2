# File: manage_jobs.py
import boto3
import json
import click
import os
import time
import subprocess


class EMRServerlessController:
    """A class to manage EMR Serverless jobs."""

    def __init__(self, project_name):
        self.project_name = project_name
        self.sts_client = boto3.client("sts")
        self.account_id = self.sts_client.get_caller_identity()["Account"]

        # Get configuration directly from Terraform state for robustness
        self.tf_outputs = self._get_terraform_outputs()
        self.aws_region = self.tf_outputs["aws_region"]["value"]
        self.s3_bucket_name = self.tf_outputs["s3_bucket_name"]["value"]
        self.kinesis_stream_name = self.tf_outputs["kinesis_stream_name"]["value"]
        self.iceberg_db_name = self.tf_outputs["iceberg_db_name"]["value"]

        # Initialize the EMR client *after* getting the region
        self.emr_client = boto3.client("emr-serverless", region_name=self.aws_region)

        self.app_id = self._get_and_start_app()
        self.execution_role_arn = f"arn:aws:iam::{self.account_id}:role/{self.project_name}-emr-serverless-role"

    def _get_terraform_outputs(self):
        """Runs `terraform output` and returns the parsed JSON."""
        print("Fetching configuration from terraform outputs...")
        try:
            # Assuming the script is run from the project root where terraform/ is a subdir
            process = subprocess.run(
                ["terraform", "output", "-json"],
                cwd="./terraform",
                capture_output=True,
                text=True,
                check=True,
                shell=True  # For Windows PATH resolution
            )
            return json.loads(process.stdout)
        except subprocess.CalledProcessError as e:
            print(f"Error running `terraform output`. Stderr: {e.stderr}")
            raise Exception("Could not fetch terraform outputs. Ensure you are in the project root and have run `terraform init/apply` in the 'terraform' directory.")
        except FileNotFoundError:
            raise Exception("`terraform` command not found. Is Terraform installed and in your PATH?")
        except json.JSONDecodeError:
            raise Exception("Failed to parse terraform output. Is the terraform state valid?")

    def _get_and_start_app(self):
        """
        Retrieves the EMR Serverless application ID by name.
        It handles CREATED, STARTED, and STOPPED states.
        If the app is STOPPED, it will start it and wait for it to be ready.
        """
        # Look for the application in any non-terminal state
        response = self.emr_client.list_applications(
            states=["CREATING", "CREATED", "STARTED", "STOPPING", "STOPPED"]
        )
        app_name_prefix = f"{self.project_name}-spark-app"

        found_app = None
        for app in response["applications"]:
            if app["name"].startswith(app_name_prefix):
                found_app = app
                break

        if not found_app:
            raise Exception(f"No EMR Serverless application found with prefix '{app_name_prefix}'. Please run `terraform apply`.")

        app_id = found_app["id"]
        app_state = found_app["state"]

        print(f"Found application {app_id} in state: {app_state}")

        if app_state == "STOPPED":
            print("Application is STOPPED. Starting it now...")
            self.emr_client.start_application(applicationId=app_id)

            # Wait for the application to become STARTED
            while app_state not in ["STARTED", "CREATED"]:
                time.sleep(10)  # Wait for 10 seconds before checking again
                app_details = self.emr_client.get_application(applicationId=app_id)
                app_state = app_details["application"]["state"]
                print(f"  ... current application state: {app_state}")
                if app_state in ["TERMINATED", "STOPPING"]:
                    raise Exception(f"Application {app_id} entered a terminal state while trying to start.")

        if app_state not in ["CREATED", "STARTED"]:
            raise Exception(f"Application {app_id} is in an unhandled state: {app_state}")

        print(f"Application {app_id} is ready.")
        return app_id

    def _get_streaming_job_config(self):
        """Returns the configuration for the streaming job."""
        return {
            "name": "Wikimedia DStream Job",
            "sparkSubmitParameters": (
                "--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,"
                "org.apache.spark:spark-streaming-kinesis-asl_2.12:3.4.1 "
                # --- Python Dependency Management ---
                # This is the key fix: Tells Spark to download our zipped helper modules
                # and add them to the PYTHONPATH so they can be imported.
                f"--py-files s3://{self.s3_bucket_name}/spark_scripts/src.zip "
                # --- Spark Configuration ---
                # These settings are passed to the Spark job.
                "--conf spark.driver.cores=2 "
                # We scale this down to 1 for a cost-effective proof-of-concept.
                "--conf spark.executor.instances=1 "
                "--conf spark.executor.cores=2 "
                "--conf spark.executor.memory=8g "
                # --- Iceberg Configuration ---
                # Tells Spark to use the Iceberg extension and sets up the Glue Catalog.
                "--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.SparkSqlExtensions "
                "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
                f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{self.s3_bucket_name}/iceberg_warehouse "
                "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
                # --- Application-specific parameters ---
                # These are retrieved inside the Spark job.
                f"--conf spark.app.kinesisStreamName={self.kinesis_stream_name} "
                f"--conf spark.app.awsRegion={self.aws_region} "
                f"--conf spark.app.icebergDbName={self.iceberg_db_name}"
            )
        }

    def start_job(self, job_type):
        """Starts a new EMR Serverless job."""
        if job_type == "streaming":
            job_driver = {
                "sparkSubmit": {
                    "entryPoint": f"s3://{self.s3_bucket_name}/spark_scripts/streaming_job.py",
                    "entryPointArguments": [],
                    "sparkSubmitParameters": self._get_streaming_job_config()["sparkSubmitParameters"]
                }
            }
            job_name = self._get_streaming_job_config()["name"]
        else:
            raise ValueError(f"Unknown job type: {job_type}")

        print(f"Starting '{job_name}' on application {self.app_id}...")
        response = self.emr_client.start_job_run(
            applicationId=self.app_id,
            executionRoleArn=self.execution_role_arn,
            jobDriver=job_driver,
            name=job_name,
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"s3://{self.s3_bucket_name}/logs/emr-serverless/{job_type}/"
                    }
                }
            }
        )
        print(f"Job started successfully! Run ID: {response['jobRunId']}")

    def get_job_status(self, run_id):
        """Gets the status of a specific job run."""
        response = self.emr_client.get_job_run(applicationId=self.app_id, jobRunId=run_id)
        print(json.dumps(response["jobRun"], indent=4, default=str))

    def cancel_job(self, run_id):
        """Cancels a running job."""
        self.emr_client.cancel_job_run(applicationId=self.app_id, jobRunId=run_id)
        print(f"Job {run_id} cancellation request sent.")


@click.group()
def cli():
    pass


@cli.command()
@click.argument('job_type', type=click.Choice(['streaming']))
def start(job_type):
    """Starts an EMR Serverless job."""
    controller = EMRServerlessController(project_name="wikimedia-trends-v2")
    controller.start_job(job_type)


@cli.command()
@click.argument('run_id')
def status(run_id):
    """Gets the status of a job run."""
    controller = EMRServerlessController(project_name="wikimedia-trends-v2")
    controller.get_job_status(run_id)


@cli.command()
@click.argument('run_id')
def cancel(run_id):
    """Cancels a job run."""
    controller = EMRServerlessController(project_name="wikimedia-trends-v2")
    controller.cancel_job(run_id)


if __name__ == '__main__':
    cli()