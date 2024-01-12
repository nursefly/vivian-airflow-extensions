import unittest
from unittest.mock import patch
from datetime import datetime

from airflow.models import DAG

from vivian_airflow_extensions.operators.stitch_operator import StitchRunAndMonitorSourceOperator

class TestStitchOperator(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag', start_date=datetime.now())
        with patch('vivian_airflow_extensions.operators.stitch_operator.StitchHook') as self.mock_stitch_hook:
            self.operator = StitchRunAndMonitorSourceOperator(
                task_id='test_task',
                source_id='test_source_id',
                client_id='test_client_id',
                conn_id='test_conn_id',
                dag=self.dag,
            )

    def test_execute(self):
        self.mock_stitch_hook.return_value._monitor_extraction.return_value = True
        self.mock_stitch_hook.return_value._get_credentials.return_value = True
        self.mock_stitch_hook.return_value._trigger_extraction.return_value = {'job_name': 'test_job'}
        self.mock_stitch_hook.return_value._get_response.return_value = True

        # Execute the operator
        self.operator.execute({})

        # Check that the StitchHook was called with the correct arguments
        self.mock_stitch_hook.assert_called_with(conn_id='test_conn_id')

    def test_trigger_extraction_success(self):
        self.mock_stitch_hook.return_value._get_credentials.return_value = True
        self.mock_stitch_hook.return_value._trigger_extraction.return_value = {'job_name': 'test_job'}

        # Execute the operator
        self.operator.execute({})

        # Check that _trigger_extraction was called with the correct arguments
        self.mock_stitch_hook.return_value._trigger_extraction.assert_called_with(source_id='test_source_id', client_id='test_client_id')

if __name__ == '__main__':
    unittest.main()