from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook


class ExtendedPostgresHook(PostgresHook): 
    """
    This class extends the PostgresHook to provide additional functionality.
    """
    @apply_defaults
    def __init__(self, postgres_conn_id='postgres_default', *args, **kwargs) -> None:
        """
        Initialize a new instance of ExtendedPostgresHook.

        :param postgres_conn_id: The ID of the connection to use.
        """
        super().__init__(*args, **kwargs)

        self.postgres_conn_id = postgres_conn_id
         
    def _run_psql_commands_in_transaction(self, commands):
        """
        Run a list of SQL commands in a single transaction.

        :param commands: The list of SQL commands to run.
        """
        conn = self.get_conn()
        cursor = conn.cursor()
        self.log.debug('[_run_psql_commands_in_transaction] -  running commands')

        try:
            for cmd in commands:
                self.log.debug('[_run_psql_commands_in_transaction] : cmd %s', cmd)
                cursor.execute(cmd)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log.critical("[_run_psql_commands_in_transaction] error: {}".format(e))
            print(cmd)
            raise e
        finally:
            self.log.debug('[_run_psql_commands_in_transaction] -  closing conn')
            conn.close()
    
    def _get_column_metadata(self, table, include_autoincrement_keys=False):
        """
        Get metadata about the columns of a table.

        :param table: The name of the table to get metadata for.
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        # get the column names from the postgres table, excluding serial columns
        column_names_sql_query = f'select column_name, data_type, column_default from information_schema.columns where table_name = \'{table}\' order by ordinal_position;'

        # get the constraints from the postgres table
        constraints_sql_query = f"""
            select
                conname, 
                pg_get_constraintdef(oid),
                contype
            from pg_constraint 
            where replace(conrelid::regclass::text, '"', '') = '{table}';
        """

        # get the indexes from the postgres table
        indexes_sql_query = f"""
            select indexname
            from pg_indexes
            where tablename = '{table}';
        """

        try:
            cursor.execute(column_names_sql_query)
            column_names_results = cursor.fetchall()

            cursor.execute(constraints_sql_query)
            constraints_results = cursor.fetchall()

            cursor.execute(indexes_sql_query)
            indexes_results = cursor.fetchall()
        except Exception as e:
            self.log.critical("[_get_column_metadata] column_names_sql_query error: {}".format(e))
            raise e
        finally:
            conn.close()

        self.serial_columns = []
        if not include_autoincrement_keys:
            for column in column_names_results:
                if column[1] == 'integer' and column[2] is not None and 'nextval' in column[2]:
                    self.serial_columns.append(column[0])
        
        self.columns_list = [column[0] for column in column_names_results if column[0] not in self.serial_columns]
        self.constraints = constraints_results
        constraints_names = [constraint[0] for constraint in constraints_results]
        self.indexes = [index[0] for index in indexes_results if index[0] not in constraints_names]

        return self.columns_list
    
    def _create_tmp_table(self, table):
        """
        Create a temporary table based on the structure of the given table.

        :param table: The name of the table to base the temporary table on.
        """
        prep_commands = []
        tmp_table = f'Tmp{table}'
        swap_table = f'Swap{table}'

        prep_commands.extend([
            f'drop table if exists "{tmp_table}";',
            f'drop table if exists "{swap_table}";',
            f'create table "{tmp_table}" (like "{table}" including all);',
        ])

        for col in self.serial_columns:
            prep_commands.append(f'alter sequence if exists "{table}_{col}_seq" owned by "{table}"."{col}";')

        # have to add the foreign keys to the new table
        for key in self.constraints:
            if key[2] == 'f':
                prep_commands.append(f'alter table if exists "{tmp_table}" add {key[1]};') 
        
        self.log.info(prep_commands)
        self._run_psql_commands_in_transaction(prep_commands)

    def _write_to_db(self, file, columns, table):
        """
        Write data from a file to a table in the database.

        :param file: The file containing the data to write.
        :param columns: The columns to write the data to.
        :param table: The table to write the data to.
        """
        file.seek(0)
        conn = self.get_conn()
        cursor = conn.cursor()
        write_to_db_sql = f"copy \"{table}\" ({columns}) from stdin with csv delimiter '|' quote '\"' header null as ''"
        self.log.info(f'writing command: {write_to_db_sql}')
        cursor.copy_expert(write_to_db_sql, file)
        conn.commit()
        conn.close()
    
    def _swap_db_tables(self, table, prep_commands=None):
        """
        Swap the names of the given table and a temporary table.

        :param table: The name of the table to swap with the temporary table.
        """
        tmp_table = f'Tmp{table}'
        swap_table = f'Swap{table}'

        if prep_commands is None:
            prep_commands = [
                f'alter table if exists "{table}" rename to "{swap_table}";',
                f'alter table if exists "{tmp_table}" rename to "{table}";',
            ]      

            for col in self.serial_columns:
                prep_commands.extend([
                    f'alter table if exists "{swap_table}" alter column "{col}" drop default;',
                    f'alter sequence if exists "{table}_{col}_seq" owned by "{table}"."{col}";'
                ])
            
            prep_commands.append(f'drop table if exists "{swap_table}";')

            # have to rename the constraints to match the old table
            for key in self.constraints:
                prep_commands.append(f'alter table if exists "{table}" rename constraint "Tmp{key[0]}" to "{key[0]}";')  
            
            # have to rename the indexes to match the old table
            for index in self.indexes:
                prep_commands.append(f'alter index if exists "Tmp{index}" rename to "{index}";')
        
        else:
            prep_commands.append(f'drop table if exists "{tmp_table}";')

        self.log.info(prep_commands)
        self._run_psql_commands_in_transaction(prep_commands)
