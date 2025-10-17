from typing import List
from tempfile import NamedTemporaryFile

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

from ..hooks.extended_snowflake_hook import ExtendedSnowflakeHook
from ..hooks.extended_postgres_hook import ExtendedPostgresHook
from ..hooks.s3_bookmark_hook import S3BookmarkHook


class SnowflakeToPostgresOperator(BaseOperator): 
    """
    This class allows for the transfer of the result of a snowflake query to a postgres table.
    """
    ui_color = '#00cdcd'
    template_fields = ['snowflake_query']

    @apply_defaults
    def __init__(self, postgres_table: str=None, snowflake_query: str=None, array_fields: list=[], snowflake_conn_id='snowflake_default', 
                 postgres_conn_id='postgres_default', schema: str='public', include_autoincrement_keys=False, *args, **kwargs) -> None:
        """
        Initialize a new instance of SnowflakeToPostgresOperator.

        :param postgres_table: The Postgres table to insert the data into.
        :param snowflake_query: The SQL query to execute in Snowflake.
        :param array_fields: The fields to treat as arrays.
        :param snowflake_conn_id: The ID of the Snowflake connection to use.
        :param postgres_conn_id: The ID of the Postgres connection to use.
        """
        super().__init__(*args, **kwargs)

        if snowflake_query is None:
            raise AirflowException('snowflake_query is required')
        if postgres_table is None:
            raise AirflowException('postgres_table is required')

        self.postgres_table = postgres_table
        self.array_fields = array_fields
        self.snowflake_query = snowflake_query
        self.insert_commands = None
        self.snowflake_conn_id = snowflake_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.schema = schema
        self.snowflake_hook = ExtendedSnowflakeHook(snowflake_conn_id=self.snowflake_conn_id, pool_pre_ping=True)
        self.postgres_hook = ExtendedPostgresHook(postgres_conn_id=self.postgres_conn_id, pool_pre_ping=True)
        self.metadata_retrieved = False
        self.include_autoincrement_keys = include_autoincrement_keys

    def execute(self, context):
        with NamedTemporaryFile('w+') as file:
            self.log.info('Fetching Snowflake data → temp file')
            new_data = self.snowflake_hook.save_snowflake_results_to_tmp_file(self.snowflake_query, self.array_fields, file, 'postgres')
            if not new_data:
                self.log.info('Snowflake query returned no rows; skipping load')
                return

            self.log.info('Retrieving Postgres table metadata for %s.%s', self.schema, self.postgres_table)
            if not self.metadata_retrieved:
                self.columns_list = self.postgres_hook.get_table_metadata(self.postgres_table, self.schema, self.include_autoincrement_keys)
                self.metadata_retrieved = True
            columns_string = ", ".join([f'"{col}"' for col in self.columns_list])

            self.log.info('Creating temp table for %s', self.postgres_table)
            self.postgres_hook.create_tmp_table(self.postgres_table)        

            self.log.info('COPY temp file → %s', f'Tmp{self.postgres_table}')
            tmp_table = f'Tmp{self.postgres_table}'
            self.postgres_hook.write_to_db(file, columns_string, tmp_table)

        self.log.info('Swapping temp → live table %s', self.postgres_table)
        self.postgres_hook.swap_db_tables(self.postgres_table, self.insert_commands)

class SnowflakeToPostgresMergeIncrementalOperator(SnowflakeToPostgresOperator):
    """
    This class extends the SnowflakeToPostgresOperator to provide functionality for merging incremental changes from Snowflake to Postgres.
    """
    template_fields = ['snowflake_query']
    
    @apply_defaults
    def __init__(self, primary_key_columns: List[str]=None, columns_to_update: List[str]=None, conditional_psql_timestamp_column: str=None, insert_only_columns: List[str]=None,
                 batch_size: int=5000, *args, **kwargs) -> None:
        """
        Initialize a new instance of SnowflakeToPostgresMergeIncrementalOperator.

        :param primary_key_columns: The primary key columns to use for merging.
        :param columns_to_update: The columns to update.
        :param conditional_psql_timestamp_column: If set, skip updating rows where
            the conditional_psql_timestamp_column in the Postgres table is ahead of
            the same column in the Snowflake table.
        :param insert_only_columns: If set, these columns will only be updated with
            new data if the existing column is null. If the row is inserted these
            columns will still be included. primary_key_columns must also be set if
            using this option, and all columns must appear in columns_to_update if
            you're also using that.
        """
        super().__init__(*args, **kwargs)

        self.primary_key_columns = primary_key_columns
        self.columns_to_update = columns_to_update
        self.conditional_psql_timestamp_column = conditional_psql_timestamp_column
        self.insert_only_columns = insert_only_columns
        self.batch_size = batch_size

    def _validate_insert_only_columns(self):
        if self.insert_only_columns is not None:
            if self.primary_key_columns is None:
                raise ValueError("insert_only_columns requires using primary_key_columns.")

            if self.columns_to_update is not None:
                for column in self.insert_only_columns:
                    if column not in self.columns_to_update:
                        raise ValueError(
                            "When using both columns_to_update and insert_only_columns, all"
                            f" insert_only_columns must appear in columns_to_update. {column}"
                            " is missing."
                        )

    def execute(self, context):
        # Build ON CONFLICT and optional WHERE clause
        if self.primary_key_columns is None:
            on_conflict_clause = ''
            conditional_timestamp_clause = ''
        else:
            if self.columns_to_update is None:
                self.columns_list = self.postgres_hook.get_table_metadata(self.postgres_table, self.schema, self.include_autoincrement_keys)
                self.metadata_retrieved = True
                self.columns_to_update = [col for col in self.columns_list if col not in self.primary_key_columns]
            self._validate_insert_only_columns()

            columns_to_update_array = []
            for column in self.columns_to_update:
                if self.insert_only_columns is not None and column in self.insert_only_columns:
                    line = f'"{column}"=coalesce("{self.postgres_table}"."{column}", excluded."{column}")'
                else:
                    line = f'"{column}"=excluded."{column}"'
                columns_to_update_array.append(line)
            columns_to_update_string = ", ".join(columns_to_update_array)
            primary_key_columns_string = ", ".join(['"' + col + '"' for col in self.primary_key_columns])
            on_conflict_clause = f'on conflict({primary_key_columns_string}) do update set {columns_to_update_string}'

            if self.conditional_psql_timestamp_column is not None:
                conditional_timestamp_clause = f'where excluded."{self.conditional_psql_timestamp_column}" >= "{self.postgres_table}"."{self.conditional_psql_timestamp_column}"'
            else:
                conditional_timestamp_clause = ''

        tmp_table = f'Tmp{self.postgres_table}'

        # Build the base insert SQL (used for single or as template for batches)
        if self.columns_to_update is None:
            base_insert_sql = f'insert into "{self.postgres_table}" select * from "{tmp_table}" {on_conflict_clause} {conditional_timestamp_clause}'
        else:
            if self.include_autoincrement_keys:
                column_list = ', '.join(['"' + col + '"' for col in self.columns_to_update])
            else:
                column_list = ', '.join(['"' + col + '"' for col in self.columns_to_update + self.primary_key_columns])
            base_insert_sql = f'insert into "{self.postgres_table}" ({column_list}) select {column_list} from "{tmp_table}" {on_conflict_clause} {conditional_timestamp_clause}'

        # If batching is enabled with a single primary key, load tmp table here and run batched upserts immediately
        if self.batch_size and self.primary_key_columns is not None and len(self.primary_key_columns) == 1:
            pk = self.primary_key_columns[0]

            from tempfile import NamedTemporaryFile
            with NamedTemporaryFile('w+') as file:
                self.log.info('Fetching Snowflake data → temp file')
                new_data = self.snowflake_hook.save_snowflake_results_to_tmp_file(self.snowflake_query, self.array_fields, file, 'postgres')
                if not new_data:
                    self.log.info('Snowflake query returned no rows; skipping load')
                    return

                self.log.info('Retrieving Postgres table metadata for %s.%s', self.schema, self.postgres_table)
                if not self.metadata_retrieved:
                    self.columns_list = self.postgres_hook.get_table_metadata(self.postgres_table, self.schema, self.include_autoincrement_keys)
                    self.metadata_retrieved = True
                columns_string = ", ".join([f'"{col}"' for col in self.columns_list])

                self.log.info('Creating temp table for %s', self.postgres_table)
                self.postgres_hook.create_tmp_table(self.postgres_table)

                self.log.info('COPY temp file → %s', tmp_table)
                self.postgres_hook.write_to_db(file, columns_string, tmp_table)

            # Execute fixed-size batches until we cover all rows in the immutable tmp table
            conn = self.postgres_hook.get_conn()
            try:
                cursor = conn.cursor()
                cursor.execute(f'select count(1) from "{tmp_table}"')
                total_rows = cursor.fetchone()[0]
            finally:
                conn.close()

            self.log.info('Batched upsert start: %d total rows, batch_size=%d, order by "%s"', total_rows, self.batch_size, pk)
            import time
            offset = 0
            batch_num = 0
            t_start = time.time()
            while offset < total_rows:
                limit = min(self.batch_size, total_rows - offset)
                batch_num += 1
                bstart = time.time()
                if self.columns_to_update is None:
                    # Insert all columns; limit rows within the SELECT subquery
                    batch_insert = (
                        f'insert into "{self.postgres_table}" '
                        f'select * from ('
                        f'  select * from "{tmp_table}" order by "{pk}" asc limit {limit} offset {offset}'
                        f') as source {on_conflict_clause} {conditional_timestamp_clause};'
                    )
                else:
                    # Insert specific columns; limit rows within the SELECT subquery
                    batch_insert = (
                        f'insert into "{self.postgres_table}" ({column_list}) '
                        f'select {column_list} from ('
                        f'  select {column_list} from "{tmp_table}" order by "{pk}" asc limit {limit} offset {offset}'
                        f') as source {on_conflict_clause} {conditional_timestamp_clause};'
                    )
                # Execute this batch as its own transaction
                self.postgres_hook._run_psql_commands_in_transaction([batch_insert])
                belapsed = time.time() - bstart
                self.log.info('Batch %d: rows %d-%d (limit=%d) in %.2fs', batch_num, offset + 1, offset + limit, limit, belapsed)
                offset += limit

            telapsed = time.time() - t_start
            self.log.info('Batched upsert complete: %d batches in %.2fs (%.0f rows/s)', batch_num, telapsed, (total_rows/telapsed) if telapsed > 0 else 0)
            # Cleanup temporary objects only (no swap needed)
            self.postgres_hook.swap_db_tables(self.postgres_table, [])
            return

        # Non-batched path: let parent handle tmp load and run a single insert command inside swap
        self.insert_commands = [base_insert_sql + ';']
        super().execute(context)


class SnowflakeToPostgresBookmarkOperator(SnowflakeToPostgresMergeIncrementalOperator):
    """
    This class extends the SnowflakeToPostgresMergeIncrementalOperator to provide functionality for transferring data from Snowflake to Postgres using bookmarks stored in s3.
    """
    template_fields = ['snowflake_query', 'bookmark_s3_key']

    @apply_defaults
    def __init__(self, incremental_key: str=None, incremental_key_type: str=None, bookmark_s3_key: str=None, *args, **kwargs) -> None:
        """
        Initialize a new instance of SnowflakeToPostgresBookmarkOperator.

        :param incremental_key: The incremental key to use.
        :param incremental_key_type: The type of the incremental key.
        :param bookmark_s3_key: The S3 key where the bookmark is stored.
        """
        super().__init__(*args, **kwargs)

        if incremental_key_type not in ('int', 'timestamp'):
            raise AirflowException(f'Key type {incremental_key_type} not supported; use int or timestamp')
        if incremental_key is None:
            raise AirflowException(f'Please provide an incremental_key')
        if bookmark_s3_key is None:
            raise AirflowException(f'Please provide a bookmark_s3_key')

        self.incremental_key = incremental_key
        self.incremental_key_type = incremental_key_type
        self.bookmark_s3_key = bookmark_s3_key

    def execute(self, context):
        self.s3_bookmark_hook = S3BookmarkHook(bookmark_s3_key=self.bookmark_s3_key, incremental_key_type=self.incremental_key_type)

        latest_bookmark = self.s3_bookmark_hook.get_latest_bookmark()
        self.snowflake_query = f'with inner_cte as ({self.snowflake_query}) select * from inner_cte where {self.incremental_key} > {latest_bookmark}'
        self.bookmark_query = f'with outer_cte as ({self.snowflake_query}) select max({self.incremental_key}) as "bookmark" from outer_cte'
        next_bookmark = self.snowflake_hook.run(sql=self.bookmark_query, handler=lambda cursor: cursor.fetchall())[0]['bookmark']

        # If no new data, skip saving bookmark and just push XComs safely
        if next_bookmark is None:
            super().execute(context)
            self.xcom_push(
                context,
                key='previous_bookmark',
                value=latest_bookmark.replace("'", "")
            )
            self.xcom_push(
                context,
                key='next_bookmark',
                value=None
            )
            return

        super().execute(context)

        self.s3_bookmark_hook.save_next_bookmark(next_bookmark)

        self.xcom_push(
            context,
            key='previous_bookmark',
            value=latest_bookmark.replace("'", "")
        )
        self.xcom_push(
            context,
            key='next_bookmark',
            value=next_bookmark.strftime(self.s3_bookmark_hook.format_string)
        )
