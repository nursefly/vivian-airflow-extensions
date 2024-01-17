import unittest
import time
from datetime import datetime
from unittest import mock
from unittest.mock import patch, MagicMock
from decimal import Decimal

from airflow.exceptions import AirflowException
from moto import mock_dynamodb
from pynamodb.models import Model
from pynamodb.attributes import UnicodeAttribute
from airflow.models import DAG

from vivian_airflow_extensions.operators.snowflake_to_dynamo_operator import SnowflakeToDynamoOperator, SnowflakeToDynamoBookmarkOperator


def clean_func(row_dict, model_columns):
    for key, value in row_dict.items():
        if isinstance(value, Decimal):
            # Convert Decimal to float, then to string
            row_dict[key] = str(float(value))
        elif isinstance(value, datetime):
            # Convert datetime object to string
            row_dict[key] = value.strftime("%Y-%m-%dT%H:%M:%S")
        elif isinstance(value, int):
            # Convert int to string
            row_dict[key] = str(value)
    return row_dict

class MockModel(Model):
    class Meta:
        table_name = "mock_table"

    id = UnicodeAttribute(hash_key=True)
    value = UnicodeAttribute()
    updatedAt = UnicodeAttribute()

class TestSnowflakeToDynamoOperator(unittest.TestCase):
    @mock.patch('vivian_airflow_extensions.operators.snowflake_to_dynamo_operator.ExtendedSnowflakeHook')
    def setUp(self, MockSnowflakeHook):
        self.snowflake_hook = MockSnowflakeHook()
        self.snowflake_hook.get_conn.return_value = mock.MagicMock()
        self.dag = DAG('test_dag', start_date=datetime.now())
        self.operator = SnowflakeToDynamoOperator(
            task_id='test_task',
            dynamo_model=MockModel,
            snowflake_query='SELECT * FROM table',
            dag=self.dag,
            cleaning_function=clean_func
        )

    def test_init(self):
        self.assertEqual(self.operator.dynamo_model, MockModel)
        self.assertEqual(self.operator.snowflake_query, 'SELECT * FROM table')
        self.assertEqual(self.operator.snowflake_conn_id, 'snowflake_default')

    def test_convert_nan(self):
        self.assertIsNone(self.operator._convert_nan(float('nan')))
        self.assertEqual(self.operator._convert_nan(1), 1)

    def test_camel_case(self):
        self.assertEqual(self.operator._camel_case('test_string'), 'testString')

    @mock_dynamodb
    def test_execute(self):
        MockModel.create_table(
            read_capacity_units=1,
            write_capacity_units=1,
            wait=True
        )

        # Add a delay to give DynamoDB time to make the table available
        time.sleep(5)

        self.snowflake_hook.generate_rows_from_table.return_value = iter([({'id': 1, 'value': str(Decimal('10.5'))}, ['id', 'value'])])
        self.operator.execute({})
        self.snowflake_hook.generate_rows_from_table.assert_called_once_with('SELECT * FROM table', 10000)

    @mock_dynamodb
    def test_query_by_id(self):
        # Create the table
        MockModel.create_table(
            read_capacity_units=1,
            write_capacity_units=1,
            wait=True
        )

        # Add a delay to give DynamoDB time to make the table available
        time.sleep(5)

        # Mock the generate_rows_from_table method to return a row with id='1' and value='10.5'
        self.snowflake_hook.generate_rows_from_table.return_value = iter([({'id': '1', 'value': '10.5'}, ['id', 'value'])])

        # Execute the operator to insert the row
        self.operator.execute({})

        # Query the table by id
        results = list(MockModel.query('1'))

        # Assert that the query returned the correct result
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].id, '1')
        self.assertEqual(results[0].value, '10.5')

        # Delete the table
        MockModel.delete_table()


class TestSnowflakeToDynamoBookmarkOperator(unittest.TestCase):
    def setUp(self):
        self.operator = SnowflakeToDynamoBookmarkOperator(
            task_id='test_task',
            snowflake_conn_id='test_snowflake_conn',
            dynamo_conn_id='test_dynamo_conn',
            snowflake_database='test_db',
            snowflake_table='test_table',
            dynamo_table='test_dynamo_table',
            incremental_key='test_key',
            incremental_key_type='int',
            bookmark_s3_key='s3://test_bucket/test_key'
        )

    def test_init(self):
        self.assertEqual(self.operator.incremental_key, 'test_key')
        self.assertEqual(self.operator.incremental_key_type, 'int')
        self.assertEqual(self.operator.bookmark_s3_key, 's3://test_bucket/test_key')

    @patch('your_module.S3BookmarkHook')  # replace with the actual module
    def test_execute(self, mock_s3_bookmark_hook):
        mock_s3_bookmark_hook.return_value._get_latest_bookmark.return_value = 1
        mock_s3_bookmark_hook.return_value._save_next_bookmark.return_value = None
        self.operator.snowflake_hook = MagicMock()
        self.operator.snowflake_hook.run.return_value = [{'bookmark': 2}]
        self.operator.execute(None)
        self.assertIn('where test_key > 1', self.operator.snowflake_query)
        mock_s3_bookmark_hook.return_value._save_next_bookmark.assert_called_once_with(2)

    @patch('your_module.S3BookmarkHook')  # replace with the actual module
    def test_execute_no_bookmark(self, mock_s3_bookmark_hook):
        mock_s3_bookmark_hook.return_value._get_latest_bookmark.return_value = None
        mock_s3_bookmark_hook.return_value._save_next_bookmark.return_value = None
        self.operator.snowflake_hook = MagicMock()
        self.operator.snowflake_hook.run.return_value = [{'bookmark': 2}]
        self.operator.execute(None)
        self.assertIn('where test_key > ', self.operator.snowflake_query)  # Check that the query uses the default value
        mock_s3_bookmark_hook.return_value._save_next_bookmark.assert_called_once_with(2)

    def test_init_invalid_incremental_key_type(self):
        with self.assertRaises(AirflowException):
            SnowflakeToDynamoBookmarkOperator(
                task_id='test_task',
                snowflake_conn_id='test_snowflake_conn',
                dynamo_conn_id='test_dynamo_conn',
                snowflake_database='test_db',
                snowflake_table='test_table',
                dynamo_table='test_dynamo_table',
                incremental_key='test_key',
                incremental_key_type='invalid',
                bookmark_s3_key='s3://test_bucket/test_key'
            )

if __name__ == '__main__':
    unittest.main()