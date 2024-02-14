import unittest
from unittest.mock import patch, MagicMock

from airflow.providers.postgres.hooks.postgres import PostgresHook

from vivian_airflow_extensions.hooks.extended_postgres_hook import ExtendedPostgresHook

class TestExtendedPostgresHook(unittest.TestCase):
    def test_init(self):
        hook = ExtendedPostgresHook(postgres_conn_id='test_conn_id')
        self.assertEqual(hook.postgres_conn_id, 'test_conn_id')

    @patch.object(PostgresHook, 'get_conn')
    def test_run_psql_commands_in_transaction(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        hook = ExtendedPostgresHook()
        hook._run_psql_commands_in_transaction(['SELECT 1'])

        mock_conn.cursor().execute.assert_called_once_with('SELECT 1')
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch.object(PostgresHook, 'get_conn')
    def test_get_table_metadata(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor().fetchall.side_effect = [[('id', 'integer', 'nextval')], [], []]

        hook = ExtendedPostgresHook()
        columns = hook.get_table_metadata('test_table')

        self.assertEqual(columns, [])
        self.assertEqual(hook.serial_columns, ['id'])

    @patch.object(ExtendedPostgresHook, '_run_psql_commands_in_transaction')
    def test_create_tmp_table(self, mock_run_psql_commands_in_transaction):
        hook = ExtendedPostgresHook()
        hook.serial_columns = ['id']
        hook.constraints = [('test_constraint', 'test_def', 'f')]

        hook.create_tmp_table('test_table')

        mock_run_psql_commands_in_transaction.assert_called_once()

    @patch.object(PostgresHook, 'get_conn')
    def test_write_to_db(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn

        hook = ExtendedPostgresHook()
        hook.write_to_db(MagicMock(), 'id', 'test_table')

        mock_conn.cursor().copy_expert.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch.object(ExtendedPostgresHook, '_run_psql_commands_in_transaction')
    def test_swap_db_tables(self, mock_run_psql_commands_in_transaction):
        hook = ExtendedPostgresHook()
        hook.serial_columns = ['id']
        hook.constraints = [('test_constraint', 'test_def', 'f')]
        hook.indexes = ['test_index']

        hook.swap_db_tables('test_table')

        mock_run_psql_commands_in_transaction.assert_called_once()

    @patch.object(PostgresHook, 'get_conn')
    def test_run_psql_commands_in_transaction_error(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor().execute.side_effect = Exception('Test exception')

        hook = ExtendedPostgresHook()

        with self.assertRaises(Exception) as context:
            hook._run_psql_commands_in_transaction(['SELECT 1'])

        self.assertTrue('Test exception' in str(context.exception))
        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch.object(PostgresHook, 'get_conn')
    def test_get_table_metadata_error(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor().execute.side_effect = Exception('Test exception')

        hook = ExtendedPostgresHook()

        with self.assertRaises(Exception) as context:
            hook.get_table_metadata('test_table')

        self.assertTrue('Test exception' in str(context.exception))
        mock_conn.close.assert_called_once()

    @patch.object(PostgresHook, 'get_conn')
    def test_write_to_db_error(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_conn.cursor().copy_expert.side_effect = Exception('Test exception')

        hook = ExtendedPostgresHook()

        with self.assertRaises(Exception) as context:
            hook.write_to_db(MagicMock(), 'id', 'test_table')

        self.assertTrue('Test exception' in str(context.exception))

if __name__ == '__main__':
    unittest.main()