from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

from vivian_airflow_extensions.operators.snowflake_to_dynamo_operator import SnowflakeToDynamoOperator, SnowflakeToDynamoBookmarkOperator
from vivian_airflow_extensions.operators.snowflake_to_postgres_operator import SnowflakeToPostgresOperator, SnowflakeToPostgresBookmarkOperator
from vivian_airflow_extensions.operators.stitch_operator import StitchRunSourceOperator, StitchRunAndMonitorSourceOperator
from vivian_airflow_extensions.sensors.stitch_sensor import StitchSensor


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2),
}

dag = DAG(
    'example_dag',
    default_args=default_args,
    description='An example DAG that uses every operator, sensor, and hook we\'ve made',
    schedule_interval=None,
)

snowflake_to_dynamo = SnowflakeToDynamoOperator(
    task_id='snowflake_to_dynamo',
    snowflake_conn_id='your_snowflake_conn',
    dynamo_conn_id='your_dynamo_conn',
    snowflake_database='your_snowflake_db',
    snowflake_table='your_snowflake_table',
    dynamo_table='your_dynamo_table',
    dag=dag,
)

snowflake_to_dynamo_bookmark = SnowflakeToDynamoBookmarkOperator(
    task_id='snowflake_to_dynamo_bookmark',
    snowflake_conn_id='your_snowflake_conn',
    dynamo_conn_id='your_dynamo_conn',
    snowflake_database='your_snowflake_db',
    snowflake_table='your_snowflake_table',
    dynamo_table='your_dynamo_table',
    incremental_key='your_incremental_key',
    incremental_key_type='your_incremental_key_type',
    bookmark_s3_key='your_bookmark_s3_key',
    dag=dag,
)

snowflake_to_postgres = SnowflakeToPostgresOperator(
    task_id='snowflake_to_postgres',
    snowflake_conn_id='your_snowflake_conn',
    postgres_conn_id='your_postgres_conn',
    snowflake_database='your_snowflake_db',
    snowflake_table='your_snowflake_table',
    postgres_table='your_postgres_table',
    dag=dag,
)

snowflake_to_postgres_bookmark = SnowflakeToPostgresBookmarkOperator(
    task_id='snowflake_to_postgres_bookmark',
    snowflake_conn_id='your_snowflake_conn',
    postgres_conn_id='your_postgres_conn',
    snowflake_database='your_snowflake_db',
    snowflake_table='your_snowflake_table',
    postgres_table='your_postgres_table',
    incremental_key='your_incremental_key',
    incremental_key_type='your_incremental_key_type',
    bookmark_s3_key='your_bookmark_s3_key',
    dag=dag,
)

stitch_run_source = StitchRunSourceOperator(
    task_id='stitch_run_source',
    source_id='your_source_id',
    client_id='your_client_id',
    conn_id='your_conn_id',
    dag=dag,
)

stitch_run_and_monitor_source = StitchRunAndMonitorSourceOperator(
    task_id='stitch_run_and_monitor_source',
    source_id='your_source_id',
    client_id='your_client_id',
    conn_id='your_conn_id',
    sleep_time=300,
    timeout=86400,
    dag=dag,
)

stitch_sensor = StitchSensor(
    task_id='stitch_sensor',
    source_id='your_source_id',
    client_id='your_client_id',
    conn_id='your_conn_id',
    poke_interval=300,
    timeout=86400,
    dag=dag,
)
