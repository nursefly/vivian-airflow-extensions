import csv

from airflow.utils.decorators import apply_defaults
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.exceptions import AirflowException


class ExtendedSnowflakeHook(SnowflakeHook): 
    """
    This class extends the SnowflakeHook to provide additional functionality.
    """
    @apply_defaults
    def __init__(self, snowflake_conn_id='snowflake_default', *args, **kwargs) -> None:
        """
        Initialize a new instance of ExtendedSnowflakeHook.

        :param snowflake_conn_id: The ID of the connection to use.
        """
        super().__init__(*args, **kwargs)

        self.snowflake_conn_id = snowflake_conn_id
    
    def generate_rows_from_table(self, query, chunk_size=1000):
        """
        Generate rows from a table in chunks.

        :param query: The SQL query to execute.
        :param chunk_size: The number of rows to fetch at a time.
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute(query)

        # Get column names once
        column_names = [desc[0] for desc in cursor.description]
        # Create a dictionary with column names as keys and None as values
        row_dict = dict.fromkeys(column_names)

        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break

            for row in rows:
                # Update values of row_dict for each row
                row_dict.update(zip(column_names, row))
                yield row_dict, column_names
    
    def save_snowflake_results_to_tmp_file(self, query, array_fields, file, destination_type='snowflake'):
        """
        Save the results of a Snowflake query to a temporary file.

        :param query: The SQL query to execute.
        :param array_fields: The fields to treat as arrays.
        :param file: The file to write the results to.
        :param destination_type: The type of the destination database.
        """
        if destination_type not in ['snowflake', 'postgres']:
            raise AirflowException(f'destination_type must be one of ["snowflake", "postgres"], not {destination_type}')

        self.log.info('START save_snowflake_results_to_tmp_file')
        self.log.info(f'Query: {query}')

        rows_generator = self.generate_rows_from_table(query)
        headers_written = False

        writer = None

        for row, headers in rows_generator:
            if not headers_written:
                writer = csv.DictWriter(file, fieldnames=headers, delimiter='|', quotechar='"')
                writer.writeheader()
                headers_written = True

            if destination_type == 'postgres':
                for key, value in row.items():
                    if key in array_fields:
                        row[key] = '{' + str(value)[1:-1] + '}'

            writer.writerow(row)

        if not headers_written:
            return False

        return True
