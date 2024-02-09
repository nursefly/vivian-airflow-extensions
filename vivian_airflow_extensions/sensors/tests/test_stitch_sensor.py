import unittest
from unittest.mock import patch
from datetime import datetime

from airflow.models import DAG

from vivian_airflow_extensions.sensors.stitch_sensor import StitchMonitorSourceSensor

class TestStitchMonitorSourceSensor(unittest.TestCase):
    def setUp(self):
        self.dag = DAG('test_dag', start_date=datetime.now())
        with patch('vivian_airflow_extensions.sensors.stitch_sensor.StitchHook') as self.mock_stitch_hook:
            self.task = StitchMonitorSourceSensor(
                task_id='test_task',
                source_id='test_source_id',
                client_id='test_client_id',
                conn_id='test_conn_id',
                dag=self.dag,
            )

    def test_poke(self):
        self.mock_stitch_hook.return_value.monitor_extraction.return_value = True
        self.mock_stitch_hook.return_value.get_credentials.return_value = True

        # Execute the operator
        self.task.poke({})

        # Check that the StitchHook was called with the correct arguments
        self.mock_stitch_hook.assert_called_with(conn_id='test_conn_id')

if __name__ == '__main__':
    unittest.main()