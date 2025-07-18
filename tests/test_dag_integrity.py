import pytest
import sys
# if the operating system is 'win32' (Windows).
if sys.platform == "win32":
    pytest.skip("Skipping Airflow DAG tests on Windows", allow_module_level=True)

from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag(mocker):
    """
    Fixture to load the DAGs. We mock the config loader *before*
    the DagBag tries to parse the DAG files.
    """
    # Mock the config object that the DAG will import
    mocker.patch('src.config_loader.config', {
        'aws': {
            'emr_serverless_app_id': 'test-app-id',
            'emr_execution_role_arn': 'test-role-arn',
            's3_bucket': 'test-bucket'
        },
        'airflow': {
            'batch_job': {
                'iceberg_db_name': 'test-db',
                'iceberg_table_name': 'test-table',
                's3_input_path_prefix': 'test/input/',
                's3_script_path_prefix': 'test/script.py'
            }
        },
        'spark': {
            'iceberg_configs': {
                'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.SparkSqlExtensions'
            }
        }
    })
    # The DagBag will now parse the DAG files using our mocked config
    return DagBag(dag_folder='dags/', include_examples=False)

def test_dag_loaded(dag_bag):
    """Assert that the DAG is loaded correctly by Airflow."""
    dag = dag_bag.get_dag(dag_id='wikimedia_batch_dag')
    assert dag_bag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1

def test_emr_operator_params(dag_bag):
    """
    Verify that the EMR Serverless operator is configured with the correct
    parameters, which are now pulled from the mocked config.
    """
    dag = dag_bag.get_dag(dag_id='wikimedia_batch_dag')
    task = dag.get_task(task_id='start_spark_batch_job')

    # Assert that the operator's attributes are set correctly
    assert task.application_id == "test-app-id"
    assert task.execution_role_arn == "test-role-arn"

    # Check the job driver configuration
    job_driver = task.job_driver['sparkSubmit']
    assert job_driver['entryPoint'] == "s3://test-bucket/test/script.py"
    assert "s3://test-bucket/test/input/" in job_driver['entryPointArguments']
    assert "glue_catalog.test-db.test-table" in job_driver['entryPointArguments']
    assert "spark.sql.catalog.glue_catalog.warehouse=s3://test-bucket/warehouse" in job_driver['sparkSubmitParameters']