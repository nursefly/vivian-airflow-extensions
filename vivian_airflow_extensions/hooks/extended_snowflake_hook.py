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
    
    def save_snowflake_results_to_tmp_file(self, query, array_fields, file, destination_type='snowflake', expected_columns=None):
        """
        Save the results of a Snowflake query to a temporary file.

        :param query: The SQL query to execute.
        :param array_fields: The fields to treat as arrays.
        :param file: The file to write the results to.
        :param destination_type: The type of the destination database.
        :param expected_columns: Optional list of expected columns. If provided and query results
            are missing some columns, they will be added with NULL values.
        """
        if destination_type not in ['snowflake', 'postgres']:
            raise AirflowException(f'destination_type must be one of ["snowflake", "postgres"], not {destination_type}')

        self.log.info('START save_snowflake_results_to_tmp_file')
        self.log.info(f'Query: {query}')

        rows_generator = self.generate_rows_from_table(query)
        headers_written = False

        writer = None
        query_columns = None
        missing_columns = []

        for row, headers in rows_generator:
            if not headers_written:
                # Convert headers to lowercase for case-insensitive comparison
                query_columns = [h.lower() for h in headers]

                # If expected_columns is provided, check for missing columns
                if expected_columns:
                    expected_lower = [c.lower() for c in expected_columns]
                    missing_columns = [col for col in expected_columns if col.lower() not in query_columns]

                    if missing_columns:
                        self.log.info(f'Adding NULL values for {len(missing_columns)} missing columns: {missing_columns}')
                        # Use expected_columns as the full fieldnames list
                        writer = csv.DictWriter(file, fieldnames=expected_columns, delimiter='|', quotechar='"')
                    else:
                        writer = csv.DictWriter(file, fieldnames=headers, delimiter='|', quotechar='"')
                else:
                    writer = csv.DictWriter(file, fieldnames=headers, delimiter='|', quotechar='"')

                writer.writeheader()
                headers_written = True

            if destination_type == 'postgres':
                for key, value in row.items():
                    if key in array_fields:
                        row[key] = '{' + str(value)[1:-1] + '}'

            # Add NULL values for missing columns
            if missing_columns:
                for col in missing_columns:
                    row[col] = None

            writer.writerow(row)

        if not headers_written:
            return False

        return True
