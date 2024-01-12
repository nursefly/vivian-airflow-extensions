import unittest
from unittest.mock import MagicMock, patch
from tempfile import NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from vivian_airflow_extensions.hooks.extended_snowflake_hook import ExtendedSnowflakeHook

class TestExtendedSnowflakeHook(unittest.TestCase):
    @patch.object(SnowflakeHook, 'get_conn')
    def test_generate_rows_from_table(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_get_conn.return_value = mock_conn
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_cursor.description = [('column1',), ('column2',)]
        mock_cursor.fetchmany.side_effect = [[('value1', 'value2')], []]

        hook = ExtendedSnowflakeHook()
        result = list(hook.generate_rows_from_table('SELECT * FROM table'))

        self.assertEqual(result, [({'column1': 'value1', 'column2': 'value2'}, ['column1', 'column2'])])

    def test_save_snowflake_results_to_tmp_file_invalid_destination_type(self):
        hook = ExtendedSnowflakeHook()

        with self.assertRaises(AirflowException) as context:
            hook.save_snowflake_results_to_tmp_file('SELECT * FROM table', [], None, 'invalid')

        self.assertTrue('destination_type must be one of ["snowflake", "postgres"], not invalid' in str(context.exception))

    @patch.object(ExtendedSnowflakeHook, 'generate_rows_from_table')
    def test_save_snowflake_results_to_tmp_file(self, mock_generate_rows_from_table):
        mock_generate_rows_from_table.return_value = iter([({'column1': 'value1', 'column2': 'value2'}, ['column1', 'column2'])])

        hook = ExtendedSnowflakeHook()
        with NamedTemporaryFile(mode='w+', delete=True) as tmp:
            result = hook.save_snowflake_results_to_tmp_file('SELECT * FROM table', [], tmp, 'snowflake')

            self.assertTrue(result)

            tmp.seek(0)  # Go back to the start of the file to read it
            lines = tmp.readlines()

        self.assertEqual(lines, ['column1|column2\n', 'value1|value2\n'])

if __name__ == '__main__':
    unittest.main()