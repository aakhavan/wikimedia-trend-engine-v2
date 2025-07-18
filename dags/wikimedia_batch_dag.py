from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
# Import our new, standardized config loader
from src.config_loader import config

# --- CONFIGURATION FROM CENTRAL FILE ---
aws_config = config['aws']
batch_config = config['airflow']['batch_job']
spark_config = config['spark']['iceberg_configs']

EMR_SERVERLESS_APPLICATION_ID = aws_config['emr_serverless_app_id']
EMR_EXECUTION_ROLE_ARN = aws_config['emr_execution_role_arn']
S3_BUCKET = aws_config['s3_bucket']

# Construct paths and table names from the config
S3_SCRIPT_PATH = f"s3://{S3_BUCKET}/{batch_config['s3_script_path_prefix']}"
S3_INPUT_PATH = f"s3://{S3_BUCKET}/{batch_config['s3_input_path_prefix']}"
ICEBERG_TABLE = f"glue_catalog.{batch_config['iceberg_db_name']}.{batch_config['iceberg_table_name']}"

# Dynamically add the S3 warehouse path to the Spark-Iceberg configurations
SPARK_ICEBERG_CONFIG = spark_config.copy()
SPARK_ICEBERG_CONFIG['spark.sql.catalog.glue_catalog.warehouse'] = f"s3://{S3_BUCKET}/warehouse"
# --- END CONFIGURATION ---

with DAG(
    dag_id="wikimedia_batch_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=["wikimedia", "emr-serverless"],
) as dag:
    start_emr_serverless_job = EmrServerlessStartJobOperator(
        task_id="start_spark_batch_job",
        application_id=EMR_SERVERLESS_APPLICATION_ID,
        execution_role_arn=EMR_EXECUTION_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": S3_SCRIPT_PATH,
                "entryPointArguments": [S3_INPUT_PATH, ICEBERG_TABLE],
                "sparkSubmitParameters": " ".join([f"--conf {k}={v}" for k, v in SPARK_ICEBERG_CONFIG.items()])
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{S3_BUCKET}/logs/emr-serverless/"
                }
            }
        },
    )