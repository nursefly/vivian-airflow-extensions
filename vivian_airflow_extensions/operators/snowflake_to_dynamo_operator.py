import json
import time
from datetime import datetime
from typing import Callable, Union, List
from decimal import Decimal

import pandas as pd
import numpy as np
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from pynamodb.models import Model

from ..hooks.extended_snowflake_hook import ExtendedSnowflakeHook
from ..hooks.s3_bookmark_hook import S3BookmarkHook


class SnowflakeToDynamoOperator(BaseOperator):
    ui_color = '#cde4ec'
    template_fields = ['snowflake_query', 'ttl_timestamp']

    @apply_defaults
    def __init__(self, dynamo_model: Model=None, snowflake_query: str=None, cleaning_function: Union[Callable, None]=None, chunksize: int=10000,
                 column_format: Union[str, None]='camel', add_updated_at: bool=True, snowflake_conn_id:str ='snowflake_default', 
                 ttl_timestamp: str=None, update_existing: bool=False, json_fields: List[str]=[], *args, **kwargs) -> None:
        """
        Runs a SQL query on Snowflake to pull data and loads the result to
        a DynamoDB table. Includes an option for specifying a
        "cleaning_function" that is applied to each row before the load.

        :param dynamo_model: The target DynamoDB model for the data load.
        :param snowflake_query: The SQL query to execute in Snowflake.
        :param cleaning_function: A function that will be applied to each row
            *after any column renaming has been completed*. This function
            must take 2 args: the row in dict format and the list of column
            names defined in the dynamo_model, and it must return 1 dict
            with the final data, formatted for the DynamoDB load. Optional.
        :param chunksize: The number of rows to pull from Snowflake at a time.
            More rows means slightly faster performance but more memory usage.
            Defaults to 10,000.
        :param column_format: Converts the column names fetched from
            Snowflake into a different format. Can be one of 'camel' (will
            convert to camel case), 'lower' (will lowercase the column names)
            or None (no formatting applied; likely will be uppercase).
            Defaults to camel.
        :param add_updated_at: Whether to add an updated_at timestamp to the data.
        :param snowflake_conn_id: The ID of the Snowflake connection to use.
        :param ttl_timestamp: If set, a new field _airflow_ttl will be added
            to the output with the value input here, cast to a number.
            Intended for use as a TTL field in DynamoDB. Defaults to None,
            which means no field is added.
        :param update_existing: If True, will update existing records based on the data
            returned from the query, rather than performing an insert. This means that,
            for records that already exist, fields that are not in the query will not
            be changed. For new records, there's no change; they will get inserted
            normally. WARNING: this is much slower than simple inserts/overwrites
            because the updates are not batched. Defaults to False.
        :param json_fields: A list of field names which contain JSON. These will be
            parsed and converted to JSON format before loading into DynamoDB. If you
            load a JSON field without including it in this list, it will be loaded as a
            string. These field names will be called *after* applying the column
            formatting (camel or lower).
        """
        super().__init__(*args, **kwargs)

        column_format_options = ['camel', 'lower', None]
        if column_format not in column_format_options:
            raise AirflowException(f'parameter column_format must be one of {column_format_options}')

        if snowflake_query is None:
            raise AirflowException(f'Please provide a snowflake_query')
        if dynamo_model is None:
            raise AirflowException(f'Please provide a dynamo_model')

        self.dynamo_model = dynamo_model
        self.snowflake_query = snowflake_query
        self.cleaning_function = cleaning_function
        self.snowflake_conn_id = snowflake_conn_id
        self.chunksize = chunksize
        self.column_format = column_format
        self.add_updated_at = add_updated_at
        self.ttl_timestamp = ttl_timestamp
        self.update_existing = update_existing
        self.json_fields = json_fields
        self.snowflake_hook = ExtendedSnowflakeHook(snowflake_conn_id=snowflake_conn_id)
        if self.column_format == 'camel':
            self.json_fields = [self._camel_case(field) for field in self.json_fields]
        else:
            self.json_fields = [field.lower() for field in self.json_fields]
        
    def _convert_nan(self, item):
        """
        Convert NaN values to None for dynamo compatibility.

        :param item: The item to check for NaN values.
        :return: The item with NaN values converted.
        """
        if isinstance(item, (list, tuple, np.ndarray, pd.Series, pd.DataFrame)):
            return item
        elif pd.isna(item):
            return None
        else:
            return item

    def _camel_case(self, string):
        """
        Convert a string to camel case.

        :param string: The string to convert.
        :return: The string in camel case.
        """
        output = ''.join(x for x in string.title() if x.isalnum())
        return output[0].lower() + output[1:]
    
    def _format_for_dynamo(self, row_dict, updated_at):
        if self.column_format == 'camel':
            row_dict = {self._camel_case(key): value for key, value in row_dict.items()}
        elif self.column_format == 'lower':
            row_dict = {key.lower(): value for key, value in row_dict.items()}

        row_dict = {key: float(value) if isinstance(value, Decimal) else value for key, value in row_dict.items()}

        if self.add_updated_at:
            if self.column_format == 'camel':
                row_dict['updatedAt'] = updated_at
            else:
                row_dict['updated_at'] = updated_at

        if self.ttl_timestamp is not None:
            row_dict['_airflow_ttl'] = float(self.ttl_timestamp)

        for field in self.json_fields:
            if pd.isna(row_dict[field]):
                row_dict[field] = None
            else:
                row_dict[field] = json.loads(row_dict[field])

        return row_dict

    def _update(self, rows_generator):
        """
        Update the DynamoDB table with the given rows.

        :param rows_generator: An generator that yields the rows to update.
        """
        model_columns = self.dynamo_model._attributes.keys()
        model_keys = [self.dynamo_model._hash_keyname]
        if self.dynamo_model._range_keyname is not None:
            model_keys.append(self.dynamo_model._range_keyname)
        key_schema = lambda x: [x.get(key) for key in model_keys]

        n = 0
        self.log.info(f'Starting UPDATE load to {self.dynamo_model}')
        updated_at = datetime.now()
        for row_dict, _ in rows_generator:
            # run the default reformatting for dynamo uploads
            row_dict = self._format_for_dynamo(row_dict, updated_at)

            # run the user-supplied cleaning function
            if self.cleaning_function is not None:
                row_dict = self.cleaning_function(row_dict, model_columns)

            # convert NaN to None
            row_dict = {key: self._convert_nan(value) for key, value in row_dict.items()}

            record = self.dynamo_model(*key_schema(row_dict))
            update_actions = [self.dynamo_model._attributes.get(key).set(value) for key, value in row_dict.items() if key not in model_keys and value is not None]
            # null attributes will be removed if set
            remove_actions = [self.dynamo_model._attributes.get(key).remove() for key, value in row_dict.items() if key not in model_keys and value is None]
            actions = update_actions + remove_actions

            tries = 3
            for attempt in range(tries):
                try:
                    record.update(actions=actions)
                except Exception as e:
                    if attempt < tries - 1:
                        self.log.info(f'An error occurred while updating the following entry. Retrying... {row_dict}')
                        time.sleep(10)
                        continue
                    else:
                        self.log.error(row_dict)
                        raise e
                break
            n += 1

        self.log.info(f'Loaded {n} rows total')

    def _insert(self, rows_generator):
        """
        Insert the given rows into the DynamoDB table.

        :param rows_generator: An generator that yields the rows to insert.
        """
        model_columns = self.dynamo_model._attributes.keys()
        n = 0
        with self.dynamo_model.batch_write() as batch_writer:
            updated_at = datetime.now()
            for row_dict, _ in rows_generator:
                # run the default reformatting for dynamo uploads
                row_dict = self._format_for_dynamo(row_dict, updated_at)

                # run the user-supplied cleaning function
                if self.cleaning_function is not None:
                    row_dict = self.cleaning_function(row_dict, model_columns)

                # convert NaN to None
                row_dict = {key: self._convert_nan(value) for key, value in row_dict.items()}

                # save results
                tries = 3
                for attempt in range(tries):
                    try:
                        batch_writer.save(self.dynamo_model(**row_dict))
                        break
                    except Exception as e:
                        if attempt < tries - 1:
                            self.log.info(f'An error occurred while inserting the following entry. Error: {e} from item {row_dict}. Retrying...')
                            time.sleep(1)
                        else:
                            self.log.error(row_dict)
                            raise e
                n += 1

        self.log.info(f'Loaded {n} rows')

    def execute(self, context):
        rows_generator = self.snowflake_hook.generate_rows_from_table(self.snowflake_query)

        if not self.update_existing:
            self._insert(rows_generator)
        else:
            self._update(rows_generator)

class SnowflakeToDynamoBookmarkOperator(SnowflakeToDynamoOperator):
    template_fields = ['snowflake_query', 'ttl_timestamp', 'bookmark_s3_key']

    def __init__(self, incremental_key: str=None, incremental_key_type: str=None, bookmark_s3_key: str=None, *args, **kwargs) -> None:
        """
        An extension of SnowflakeToDynamoOperator, this operator lets the user
        specify a field in the Snowflake table to use as the incremental key. On
        each run, this operator will read the previous incremental key and only
        load rows with incremental key greater than the last key. Typically this
        key should be either an updatedAt field or an auto-incremented ID for
        insert-only tables.

        :param incremental_key: The incremental key, one of the fields of the Snowflake table
        :param incremental_key_type: The type of the incremental key field. Must be eitherint or timestamp
        :param bookmark_s3_key: The s3 path to save the bookmarks to. Must be in the format s3://{bucket}/{your key}
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
        s3_bookmark_hook = S3BookmarkHook(bookmark_s3_key=self.bookmark_s3_key, incremental_key_type=self.incremental_key_type)
        latest_bookmark = s3_bookmark_hook.get_latest_bookmark()
        self.snowflake_query = f'with inner_cte as ({self.snowflake_query}) select * from inner_cte where {self.incremental_key} > {latest_bookmark}'
        bookmark_query = f'with outer_cte as ({self.snowflake_query}) select max({self.incremental_key}) as "bookmark" from outer_cte'
        next_bookmark = self.snowflake_hook.run(sql=bookmark_query, handler=lambda cursor: cursor.fetchall())[0]['bookmark']

        super().execute(context)

        s3_bookmark_hook.save_next_bookmark(next_bookmark) 
