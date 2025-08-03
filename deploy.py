# File: deploy.py
import boto3
import subprocess
import json
import logging
import shutil
import os

# --- Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
SOURCE_DIR = "src"
ZIP_FILE_NAME = "src.zip"
SPARK_SCRIPT_FILE = "streaming_job.py"


# --- Helper Functions ---
def get_terraform_outputs():
    """Runs `terraform output` and returns the parsed JSON."""
    logging.info("Fetching outputs from Terraform...")
    try:
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
        logging.error(f"\n❌ Error running `terraform output`. Stderr: {e.stderr}")
        raise Exception("Could not fetch terraform outputs.")
    except FileNotFoundError:
        raise Exception("`terraform` command not found. Is Terraform installed and in your PATH?")


def zip_source_code(output_filename, source_dir):
    """Zips the source directory."""
    logging.info(f"Zipping source directory '{source_dir}' to '{output_filename}'...")
    # Use shutil to create a zip file. It's platform-independent.
    shutil.make_archive(base_name=output_filename.replace('.zip', ''), format='zip', root_dir=source_dir)
    logging.info("✅ Zipping complete.")


def upload_to_s3(s3_client, bucket_name, local_file_path, s3_key):
    """Uploads a file to a specified S3 bucket."""
    logging.info(f"Uploading '{local_file_path}' to 's3://{bucket_name}/{s3_key}'...")
    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        logging.info("✅ Upload successful.")
    except Exception as e:
        logging.error(f"❌ Failed to upload {local_file_path}: {e}")
        raise


# --- Main Deployment Logic ---
def main():
    """Main deployment function."""
    try:
        # 1. Get S3 bucket name from Terraform state
        tf_outputs = get_terraform_outputs()

        # This is the key change: use the standardized output name
        if "s3_bucket_name" not in tf_outputs:
            raise KeyError("Could not find 's3_bucket_name' in Terraform outputs.")
        bucket_name = tf_outputs["s3_bucket_name"]["value"]

        if "aws_region" not in tf_outputs:
            raise KeyError("Could not find 'aws_region' in Terraform outputs.")
        aws_region = tf_outputs["aws_region"]["value"]

        s3_client = boto3.client("s3", region_name=aws_region)

        # 2. Zip the source code in the 'src' directory
        # The main script is already included in the zip, but uploading it separately
        # makes it easier to reference as the job's entry point.
        zip_source_code(ZIP_FILE_NAME, SOURCE_DIR)

        # 3. Upload the zipped source code
        upload_to_s3(
            s3_client,
            bucket_name,
            ZIP_FILE_NAME,
            f"spark_scripts/{ZIP_FILE_NAME}"
        )

        # 4. Upload the main Spark job script
        upload_to_s3(
            s3_client,
            bucket_name,
            os.path.join(SOURCE_DIR, SPARK_SCRIPT_FILE),
            f"spark_scripts/{SPARK_SCRIPT_FILE}"
        )

        logging.info("\n🚀 Deployment finished successfully! 🚀")

    except (KeyError, Exception) as e:
        logging.error(f"\n❌ Deployment failed: {e}")
    finally:
        # Clean up the created zip file
        if os.path.exists(ZIP_FILE_NAME):
            os.remove(ZIP_FILE_NAME)
            logging.info(f"Cleaned up local file: {ZIP_FILE_NAME}")


if __name__ == "__main__":
    main()