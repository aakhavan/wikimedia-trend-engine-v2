import subprocess
import json
import yaml
import boto3
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_terraform_outputs() -> dict:
    """
    Runs 'terraform output -json' and returns the parsed outputs.
    """
    logging.info("Fetching outputs from Terraform...")
    try:
        # We assume this script is run from the project root, and terraform is in a sub-directory
        result = subprocess.run(
            ["terraform", "output", "-json"],
            capture_output=True,
            text=True,
            check=True,
            cwd="./terraform"  # Run command in the terraform directory
        )
        outputs = json.loads(result.stdout)
        # The output values are nested, so we extract them
        return {key: value['value'] for key, value in outputs.items()}
    except (subprocess.CalledProcessError, FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to get Terraform outputs: {e}")
        raise


def update_config(tf_outputs: dict) -> dict:
    """
    Loads the base config.yml, updates it with Terraform outputs, and returns it.
    """
    logging.info("Updating local configuration with Terraform outputs...")
    config_path = Path("config/config.yml")
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    # --- FIX: Align these keys with the names in terraform/outputs.tf ---
    config['aws']['s3_bucket'] = tf_outputs.get('data_lake_s3_bucket_name')
    config['aws']['emr_serverless_app_id'] = tf_outputs.get('emr_serverless_app_id')
    config['aws']['emr_execution_role_arn'] = tf_outputs.get('emr_execution_role_arn')

    # Validate that all values were updated
    for key, value in config['aws'].items():
        if value is None or "<" in str(value):
            raise ValueError(f"Configuration key 'aws.{key}' was not updated correctly.")

    return config


def upload_to_s3(s3_bucket: str, updated_config: dict):
    """
    Uploads necessary scripts and the updated configuration to S3.
    """
    s3_client = boto3.client("s3")

    # --- Upload Scripts ---
    # We should also upload the producer script in case we want to run it from a cloud machine
    scripts_to_upload = [
        "src/batch_job.py",
        "src/streaming_job.py",
        "src/transformations.py",
        "src/config_loader.py",
        "src/producer.py"
    ]

    logging.info(f"Uploading scripts to s3://{s3_bucket}/spark_scripts/")
    for script_path_str in scripts_to_upload:
        script_path = Path(script_path_str)
        s3_key = f"spark_scripts/{script_path.name}"
        s3_client.upload_file(str(script_path), s3_bucket, s3_key)
        logging.info(f"  ✓ Uploaded {script_path.name}")

    # --- Upload Updated Config ---
    logging.info(f"Uploading updated configuration to s3://{s3_bucket}/config/")
    config_s3_key = "config/config.yml"
    # Convert the updated config dict back to a YAML string and upload
    updated_config_yaml = yaml.dump(updated_config)
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=config_s3_key,
        Body=updated_config_yaml.encode('utf-8')
    )
    logging.info("  ✓ Uploaded config.yml")


def main():
    """Main deployment orchestration function."""
    try:
        # Step 1: Get outputs from Terraform
        tf_outputs = get_terraform_outputs()

        # Step 2: Update the configuration object in memory
        updated_config = update_config(tf_outputs)
        s3_bucket = updated_config['aws']['s3_bucket']

        # Step 3: Upload all necessary files to S3
        upload_to_s3(s3_bucket, updated_config)

        logging.info("\n✅ Deployment successful!")
        logging.info("Application scripts and configuration have been uploaded to S3.")
        logging.info("You can now start the Kinesis producer and the EMR Serverless jobs.")

    except (Exception) as e:
        logging.error(f"\n❌ Deployment failed: {e}")
        return 1


if __name__ == "__main__":
    main()