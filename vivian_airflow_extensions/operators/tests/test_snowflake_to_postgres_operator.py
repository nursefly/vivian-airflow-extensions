import unittest
from datetime import datetime
from unittest.mock import patch, ANY

from airflow.models import DAG

from vivian_airflow_extensions.operators.snowflake_to_postgres_operator import SnowflakeToPostgresOperator
from vivian_airflow_extensions.operators.snowflake_to_postgres_operator import SnowflakeToPostgresBookmarkOperator

class TestSnowflakeToPostgresOperator(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(dag_id='test_dag', start_date=datetime.now())
        with patch('vivian_airflow_extensions.operators.snowflake_to_postgres_operator.ExtendedSnowflakeHook') as self.mock_snowflake_hook, \
             patch('vivian_airflow_extensions.operators.snowflake_to_postgres_operator.ExtendedPostgresHook') as self.mock_postgres_hook:
            self.operator = SnowflakeToPostgresOperator(
                task_id='test_task',
                snowflake_query='SELECT * FROM table',
                postgres_table='postgres_table',
                array_fields=['column2'],
                dag = self.dag
            )

    def test_execute(self):
        # Mock the Snowflake hook and its methods
        self.mock_snowflake_hook.return_value.generate_rows_from_table.return_value = iter([
            ({'column1': 1, 'column2': 'data1'}, ['column1', 'column2']),
            ({'column1': 2, 'column2': 'data2'}, ['column1', 'column2']),
        ])

        # Execute the operator
        self.operator.execute({})

        # Assert the expected interactions with Snowflake hook
        self.mock_snowflake_hook.assert_called_once_with(snowflake_conn_id='snowflake_default', pool_pre_ping=True)
        self.mock_snowflake_hook.return_value.save_snowflake_results_to_tmp_file.assert_any_call(self.operator.snowflake_query, self.operator.array_fields, ANY, 'postgres')

        # Assert the expected interactions with Postgres hook
        self.mock_postgres_hook.assert_called_once_with(postgres_conn_id='postgres_default', pool_pre_ping=True)
        self.mock_postgres_hook.return_value._get_column_metadata.assert_called_once_with(self.operator.postgres_table)
        self.mock_postgres_hook.return_value._create_tmp_table.assert_called_once_with(self.operator.postgres_table)
        self.mock_postgres_hook.return_value._write_to_db.assert_called_once_with(ANY, ANY, f'Tmp{self.operator.postgres_table}')
        self.mock_postgres_hook.return_value._swap_db_tables.assert_called_once_with(self.operator.postgres_table)

class TestSnowflakeToPostgresBookmarkOperator(unittest.TestCase):
    def setUp(self):
        self.dag = DAG(dag_id='test_dag', start_date=datetime.now())
        with patch('vivian_airflow_extensions.operators.snowflake_to_postgres_operator.ExtendedSnowflakeHook') as self.mock_snowflake_hook, \
             patch('vivian_airflow_extensions.operators.snowflake_to_postgres_operator.ExtendedPostgresHook') as self.mock_postgres_hook, \
             patch('vivian_airflow_extensions.operators.snowflake_to_postgres_operator.S3BookmarkHook') as self.mock_s3_bookmark_hook:
            self.operator = SnowflakeToPostgresBookmarkOperator(
                task_id="test_task1",
                postgres_table="my_table",
                snowflake_query="SELECT * FROM my_snowflake_table",
                dag=self.dag,
                primary_key_columns=['id'],
                incremental_key='updated_at',
                incremental_key_type='timestamp',
                bookmark_s3_key='s3://nursefly-airflow/bookmarks/{{ var.value.environment }}/{{ dag.dag_id }}/{{ task.task_id }}/bookmark.txt',
            )
    
    def test_execute(self):
        # Mock the Snowflake hook
        self.mock_snowflake_hook.return_value.generate_rows_from_table.return_value = iter([
            ({'column1': 1, 'column2': 'data1'}, ['column1', 'column2']),
            ({'column1': 2, 'column2': 'data2'}, ['column1', 'column2']),
        ])

        # Mock the S3BookmarkHook
        self.mock_s3_bookmark_hook.return_value._get_latest_bookmark.return_value = '2021-01-11 12:00:00.000'

        # Execute the operator
        self.operator.execute({})

        # Assert the expected interactions with Snowflake hook
        self.mock_snowflake_hook.assert_called_once_with(snowflake_conn_id='snowflake_default', pool_pre_ping=True)
        self.mock_snowflake_hook.return_value.save_snowflake_results_to_tmp_file.assert_any_call(self.operator.snowflake_query, self.operator.array_fields, ANY, 'postgres')

        # Assert the expected interactions with Postgres hook
        self.mock_postgres_hook.assert_called_once_with(postgres_conn_id='postgres_default', pool_pre_ping=True)
        self.mock_postgres_hook.return_value._get_column_metadata.assert_called_with(self.operator.postgres_table)
        self.mock_postgres_hook.return_value._create_tmp_table.assert_called_once_with(self.operator.postgres_table)
        self.mock_postgres_hook.return_value._write_to_db.assert_called_once_with(ANY, ANY, f'Tmp{self.operator.postgres_table}')
        self.mock_postgres_hook.return_value._swap_db_tables.assert_called_once_with(self.operator.postgres_table)

        # Assert the generated Snowflake queries
        expected_snowflake_query = 'with inner_cte as (SELECT * FROM my_snowflake_table) select * from inner_cte where updated_at > 2021-01-11 12:00:00.000'
        expected_bookmark_query = 'with outer_cte as (with inner_cte as (SELECT * FROM my_snowflake_table) select * from inner_cte where updated_at > 2021-01-11 12:00:00.000) select max(updated_at) as "bookmark" from outer_cte'
        self.assertEqual(self.operator.snowflake_query, expected_snowflake_query)
        self.assertEqual(self.operator.bookmark_query, expected_bookmark_query)
        
if __name__ == '__main__':
    unittest.main()
