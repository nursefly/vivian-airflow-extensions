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
        self.log.info('\n' + '\n'.join(f"    {command}" for command in commands))

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
    
    def _generate_drop_table_attributes_commands(self, table):
        """
        Generates SQL commands to drop attributes of a table.

        This function generates SQL commands to drop the constraints, indexes, 
        and sequences associated with a given table. It is intended to be used 
        when you want to recreate a table with different attributes.

        :param table: The name of the table for which to generate the drop commands.
        :type table: str

        :return: A list of SQL commands to drop the table's constraints, indexes, and sequences.
        :rtype: list of str
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        # Fetch all constraint names from the table
        cursor.execute(f"""
            select conname
            from pg_constraint 
            inner join pg_class on conrelid=pg_class.oid 
            where relname='{table}';
        """)

        self.drop_constraint_commands = [f'alter table "{table}" drop constraint if exists "{row[0]}";' for row in cursor.fetchall() if row[0]]

        # Fetch all index names from the table
        cursor.execute(f"""
            select indexname
            from pg_indexes 
            where tablename='{table}';
        """)

        self.drop_index_commands = [f'drop index if exists "{row[0]}";' for row in cursor.fetchall() if row[0]]
    
    def _get_table_columns(self, table, cursor):
        columns_sql_query = f"""
            select 
                column_name, 
                data_type, 
                column_default 
            from information_schema.columns 
            where table_name = '{table}';
        """

        cursor.execute(columns_sql_query)
        columns_results = cursor.fetchall()
        return columns_results

    def _get_table_constraints(self, table, cursor):
        constraints_sql_query = f"""
            select
                conname, 
                pg_get_constraintdef(oid),
                contype
            from pg_constraint 
            where replace(conrelid::regclass::text, '"', '') = '{table}';
        """

        cursor.execute(constraints_sql_query)
        constraints_results = cursor.fetchall()
        return constraints_results

    def _get_table_indexes(self, table, cursor):
        indexes_sql_query = f"""
            select indexname, indexdef
            from pg_indexes
            where tablename = '{table}';
        """

        cursor.execute(indexes_sql_query)
        indexes_results = cursor.fetchall()
        return indexes_results

    def _get_table_sequences(self, cursor, schema, columns_results):
        """
        Retrieves the sequences associated with a given table.

        This function queries the PostgreSQL system catalogs to find sequences 
        that are associated with the specified table. Sequences are database objects 
        that are often used to create unique identifiers for rows in a table.

        :param table: The name of the table for which to retrieve the sequences.
        :type table: str

        :return: A list of sequences associated with the table.
        :rtype: list of str
        """
        self.sequences = []
        for column in columns_results:
            # Checks if the column is a sequence
            if column[1] in ['integer', 'smallint', 'bigint'] and column[2] is not None and 'nextval' in column[2]:
                seq_results = column[2].split("'")[1]
                seq_name = seq_results.split('.')[-1].strip('"')

                # Get the existing sequence's properties
                cursor.execute(f"""
                    select sequencename, increment_by, min_value, max_value, last_value, cycle, 
                    from pg_sequences 
                    where schemaname = '{schema}' and sequencename = '{seq_name}';
                """)
                seq_properties = cursor.fetchone()

                self.sequences.append({
                    'name': seq_properties[0],
                    'column': column[0],
                    'increment': seq_properties[1],
                    'minvalue': seq_properties[2],
                    'maxvalue': seq_properties[3],
                    'last': seq_properties[4],
                    'cycle': ' cycle' if seq_properties[5] else '',
                })

    def get_table_metadata(self, table, schema, include_autoincrement_keys):
        """
        Retrieves metadata for a given table.

        This function retrieves various types of metadata for the specified table, 
        including the schema, columns, constraints, indexes, and sequences. It uses 
        several helper functions to retrieve each type of metadata.

        :param table: The name of the table for which to retrieve the metadata.
        :type table: str

        :return: None. The metadata is stored in instance variables.
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        try:
            columns_results = self._get_table_columns(table, cursor)
            constraints_results = self._get_table_constraints(table, cursor)
            indexes_results = self._get_table_indexes(table, cursor)
            self._get_table_sequences(cursor, schema, columns_results)
        except Exception as e:
            self.log.critical("[get_table_metadata] error: {}".format(e))
            raise e
        finally:
            conn.close()
        
        self.constraints = [{'name': row[0], 'definition': row[1], 'type': row[2]} for row in constraints_results]

        constraints_names = [constraint[0] for constraint in constraints_results]  
        self.indexes = [{'name': index[0], 'definition': index[1]} for index in indexes_results if index[0] not in constraints_names]

        if include_autoincrement_keys:
            columns_list = [column[0] for column in columns_results]
        else:
            columns_list = [column[0] for column in columns_results if column[0] not in [seq['column'] for seq in self.sequences]]

        return columns_list
    
    def create_tmp_table(self, table):
        """
        Creates a temporary table in the database.

        This function creates a temporary table in the database with the same 
        structure as the specified table. Temporary tables are useful for 
        performing operations that require a temporary workspace.

        :param table: The name of the table to use as a template for the temporary table.
        :type table: str

        :return: None. The temporary table is created in the database.
        """

        prep_commands = []
        self.tmp_table = f'Tmp{table}'
        self.swap_table = f'Swap{table}'

        # Drop old temp and swap tables if they exist
        prep_commands.extend([
            f'drop table if exists "{self.tmp_table}" cascade;',
            f'drop table if exists "{self.swap_table}" cascade;',
            f'create table "{self.tmp_table}" (like "{table}" including all);'
        ])
        
        # Create a temporary sequence for each sequence in the original table
        for seq in self.sequences:
            sequence_name = seq['name']
            tmp_sequence_name = f'Tmp{sequence_name}'
            prep_commands.extend([
                f'drop sequence if exists "{tmp_sequence_name}" cascade;',
                f'create sequence "{tmp_sequence_name}" increment {seq["increment"]} minvalue {seq["minvalue"]} maxvalue {seq["maxvalue"]} last_value {seq["last"]} {seq["cycle"]};',
                f'alter table "{self.tmp_table}" alter column "{seq["column"]}" set default nextval(\'"{tmp_sequence_name}"\');'
            ])

        # Create a temporary foreign key for each foreign key in the original table
        for constraint in self.constraints:
            if constraint['type'] == 'f':
                prep_commands.append(f'alter table "{self.tmp_table}" add constraint "Tmp{constraint["name"]}" {constraint["definition"]};')

        self._run_psql_commands_in_transaction(prep_commands)
        self._generate_drop_table_attributes_commands(self.tmp_table)

    def write_to_db(self, file, columns_string, table):
        """
        Write data from a file to a table in the database.

        :param file: The file containing the data to write.
        :param columns_string: The columns to write the data to.
        :param table: The table to write the data to.
        """
        file.seek(0)
        conn = self.get_conn()
        cursor = conn.cursor()

        self.log.info(table)
        self.log.info(columns_string)

        write_to_db_sql = f'copy "{table}" ({columns_string}) from stdin with csv delimiter \'|\' quote \'"\' header null as \'\''
        
        self.log.info(f'writing command: {write_to_db_sql}')
        cursor.copy_expert(write_to_db_sql, file)
        conn.commit()
        conn.close()
    
    def swap_db_tables(self, table, prep_commands=None):
        """
        Swap the given table and a temporary table.

        :param table: The name of the table to swap with the temporary table.
        :param prep_commands: The list of SQL commands to run. If this is not null, most of the logic gets skipped and the function simply deletes leftover temp tables and sequences
        """

        if prep_commands is None:
            prep_commands = []

            # Drop the temporary constraints and indexe
            prep_commands.extend(self.drop_constraint_commands)
            prep_commands.extend(self.drop_index_commands)
            
            for seq in self.sequences:
                prep_commands.append(f'drop sequence if exists "{seq["name"]}" cascade;')

            # Swap the tables and drop the old table
            prep_commands.extend([
                f'alter table if exists "{table}" rename to "{self.swap_table}";',
                f'alter table if exists "{self.tmp_table}" rename to "{table}";',
                f'drop table if exists "{self.swap_table}" cascade;'
            ])

            # Add the correctly named constraints, indexes, and sequences
            for constraint in self.constraints:
                prep_commands.append(f'alter table if exists "{table}" add constraint "{constraint["name"]}" {constraint["definition"]};')

            for index in self.indexes:
                prep_commands.append(f'create index "{index["name"]}" on "{table}" {index["definition"]};')
            
            for seq in self.sequences:
                sequence_name = seq['name']
                tmp_sequence_name = f'Tmp{sequence_name}'
                prep_commands.extend([
                    f'alter sequence "{tmp_sequence_name}" rename to "{sequence_name}";'
                    f'alter table "{table}" alter column "{seq["column"]}" set default nextval(\'"{sequence_name}"\');'
                ])
        
        else:
            prep_commands.append(f'drop table if exists "{self.tmp_table}" cascade;')
            for seq in self.sequences:
                sequence_name = seq['name']
                tmp_sequence_name = f'Tmp{sequence_name}'
                prep_commands.append(f'drop sequence if exists "{tmp_sequence_name}" cascade;')

        self._run_psql_commands_in_transaction(prep_commands)
