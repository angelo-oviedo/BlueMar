from airflow.decorators import task
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.amazon.aws.operators.lambda_function import AwsLambdaInvokeFunctionOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import json

# DAG setup
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

AIRBYTE_CONNECTION_ID = Variable.get("MY_EXAMPLE_CONNECTION_ID")
API_KEY = f'Bearer {Variable.get("CLOUD_API_TOKEN")}'
AWS_CONN_ID = Variable.get("AWS_CONN_ID")
STREAMLIT_REFRESH_ENDPOINT = "https://app.snowflake.com/wndprcz/kub77776/#/streamlit-apps/BLUEMAR_DEV_DB.BLUEMAR_DEV_DBT_SCHEMA_MODELED.GOSI0E55XUC26GM2/refresh"

def trigger_streamlit_refresh():
    """
    Call Streamlit endpoint to refresh data
    """
    import requests
    response = requests.post(STREAMLIT_REFRESH_ENDPOINT)
    response.raise_for_status()

with DAG(
    'bluemar_pipeline_dag',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['etl', 'airbyte', 'dbt', 'snowflake', 'soda', 'streamlit']
) as dag:

    start = DummyOperator(task_id="start")

    # Airbyte sync
    trigger_airbyte_sync = SimpleHttpOperator(
        method="POST",
        task_id='start_airbyte_sync',
        http_conn_id='airbyte-api-cloud-connection',
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",
            "Accept": "application/json",
            "Authorization": API_KEY
        },
        endpoint='/v1/jobs',
        data=json.dumps({"connectionId": AIRBYTE_CONNECTION_ID, "jobType": "sync"}),
        do_xcom_push=True,
        response_filter=lambda response: response.json()['jobId'],
        log_response=True,
    )

    wait_for_sync_to_complete = HttpSensor(
        method='GET',
        task_id='wait_for_airbyte_sync',
        http_conn_id='airbyte-api-cloud-connection',
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fake-useragent",
            "Accept": "application/json",
            "Authorization": API_KEY
        },
        endpoint='/v1/jobs/{}'.format("{{ task_instance.xcom_pull(task_ids='start_airbyte_sync') }}"),
        poke_interval=5,
        response_check=lambda response: json.loads(response.text)['status'] == "succeeded"
    )

    # Lambda preprocessing
    trigger_lambda = AwsLambdaInvokeFunctionOperator(
        task_id="trigger_lambda_preprocessing",
        function_name="bluemar_lambda_preprocessing",
        payload='{}',
        aws_conn_id=AWS_CONN_ID,
    )

    # Snowflake load
    load_to_snowflake = SnowflakeOperator(
        task_id="load_to_snowflake",
        sql="""
        COPY INTO BLUEMAR_DEV_DB.RAW.PROCESSED_GASTOS_OPERATIVOS
        FROM @bluemar_raw_data_external_stage/Gastos_operativos/
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE';

        COPY INTO BLUEMAR_DEV_DB.RAW.PROCESSED_INGRESOS
        FROM @bluemar_raw_data_external_stage/Ingresos/
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE';

        COPY INTO BLUEMAR_DEV_DB.RAW.PROCESSED_PAGO_PLANILLAS
        FROM @bluemar_raw_data_external_stage/Pago_planillas/
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE';

        COPY INTO BLUEMAR_DEV_DB.RAW.PROCESSED_INVENTARIO_DE_ACTIVOS
        FROM @bluemar_raw_data_external_stage/Inventario_de_activos/
        FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"')
        ON_ERROR = 'CONTINUE';
        """,
        snowflake_conn_id="snowflake_default",
    )

    # dbt transformations
    dbt_transform_raw = BashOperator(
        task_id="dbt_transform_raw_layer",
        bash_command="cd BlueMar/dags/dbt/BlueMar && dbt run --select raw/*"
    )
    
    dbt_transform_stage = BashOperator(
        task_id="dbt_transform_staging_layer",
        bash_command="cd BlueMar/dags/dbt/BlueMar && dbt run --select stage/*"
    )

    # Soda check using external Python
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        from soda.check_function import check
        return check(scan_name, checks_subpath)
    
    soda_check = check_load()

    dbt_transform_modeled = BashOperator(
        task_id="dbt_transform_modeled_layer",
        bash_command="cd BlueMar/dags/dbt/BlueMar && dbt run --select modeled/*"
    )
    
    streamlit_refresh = PythonOperator(
        task_id="streamlit_dashboard_refresh",
        python_callable=trigger_streamlit_refresh
    )

    end = DummyOperator(task_id="end")

    # DAG dependencies
    start >> trigger_airbyte_sync >> wait_for_sync_to_complete
    wait_for_sync_to_complete >> trigger_lambda >> load_to_snowflake
    load_to_snowflake >> dbt_transform_raw >> dbt_transform_stage >> soda_check
    soda_check >> dbt_transform_modeled >> streamlit_refresh >> end
