import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from airflow.exceptions import AirflowException

from vivian_airflow_extensions.hooks.stitch_hook import StitchHook

class TestStitchHook(unittest.TestCase):
    @patch.object(StitchHook, 'get_connection')
    def test_get_credentials(self, mock_get_connection):
        conn = MagicMock()
        conn.host = 'test_host'
        conn.extra_dejson = {'extra': 'data'}
        mock_get_connection.return_value = conn

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook._get_credentials()

        self.assertEqual(stitch_hook.host, 'test_host')
        self.assertEqual(stitch_hook.headers, {'extra': 'data'})

    @patch('requests.request')
    def test_get_response(self, mock_request):
        mock_response = MagicMock()
        mock_response.text = '{"key": "value"}'
        mock_request.return_value = mock_response

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'
        stitch_hook.headers = {'extra': 'data'}

        response = stitch_hook._get_response('test_url', 'GET')

        self.assertEqual(response, {'key': 'value'})

    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_trigger_extraction(self, mock_get_response, mock_get_credentials):
        mock_get_response.return_value = {'job_name': 'test_job_name'}

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        stitch_hook._trigger_extraction('test_source_id', 'test_client_id')

        mock_get_response.assert_called_once_with(f'{stitch_hook.host}/sources/test_source_id/sync', 'POST')

    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_trigger_extraction_error(self, mock_get_response, mock_get_credentials):
        mock_get_response.return_value = {'error': {'type': 'test_type', 'message': 'test_message'}}

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        with self.assertRaises(AirflowException):
            stitch_hook._trigger_extraction('test_source_id', 'test_client_id')

    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_trigger_extraction_no_job_name(self, mock_get_response, mock_get_credentials):
        mock_get_response.return_value = {}

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        with self.assertRaises(AirflowException):
            stitch_hook._trigger_extraction('test_source_id', 'test_client_id')

    @patch('time.sleep', return_value=None)
    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_monitor_extraction(self, mock_get_response, mock_get_credentials, mock_sleep):
        mock_get_response.return_value = {
            'data': [{'source_id': 'test_source_id', 'completion_time': '2022-01-01T00:00:00Z', 'tap_exit_status': 0}],
            'links': {}
        }

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        stitch_hook._monitor_extraction('test_source_id', 'test_client_id', start_time=datetime(2022, 1, 1))

        mock_get_response.assert_called()

    @patch('time.sleep', return_value=None)
    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_monitor_extraction_source_id_not_found(self, mock_get_response, mock_get_credentials, mock_sleep):
        mock_get_response.return_value = {'data': [], 'links': {}}

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        with self.assertRaises(AirflowException):
            stitch_hook._monitor_extraction('test_source_id', 'test_client_id', start_time=datetime(2022, 1, 1))

    @patch('time.sleep', return_value=None)
    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_monitor_extraction_extraction_failed(self, mock_get_response, mock_get_credentials, mock_sleep):
        mock_get_response.return_value = {
            'data': [{'source_id': 'test_source_id', 'completion_time': '2022-01-01T00:00:00Z', 'tap_exit_status': 1}],
            'links': {}
        }

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        with self.assertRaises(AirflowException):
            stitch_hook._monitor_extraction('test_source_id', 'test_client_id', start_time=datetime(2022, 1, 1))

    @patch('time.sleep', return_value=None)
    @patch.object(StitchHook, '_get_credentials')
    @patch.object(StitchHook, '_get_response')
    def test_monitor_extraction_timeout(self, mock_get_response, mock_get_credentials, mock_sleep):
        mock_get_response.return_value = {
            'data': [{'source_id': 'test_source_id', 'completion_time': '2021-12-31T23:59:59Z', 'tap_exit_status': 0}],
            'links': {}
        }

        stitch_hook = StitchHook(conn_id='test_conn_id')
        stitch_hook.host = 'test_host'

        with self.assertRaises(AirflowException):
            stitch_hook._monitor_extraction('test_source_id', 'test_client_id', start_time=datetime(2022, 1, 1), timeout=1)

if __name__ == '__main__':
    unittest.main()