# Open-sourced Airflow Addons by Vivian Health 
This repository contains custom operators and sensors for Apache Airflow. These operators and sensors are designed to facilitate data transfer between different systems such as Snowflake, DynamoDB, PostgreSQL, and Stitch.

## Setup

Airflow needs to be able to connect to Snowflake, Postgres, Stitch, and DyanmoDB. This requires setting up connections which get passed to the operators. Airflow handles these through the `connections` settings. You can also use the following JSON blobs in AWS Parameter store, or other secret managers.

### Snowflake

```
{
    "conn_type": "snowflake",
    "login": "<YOUR LOGIN>",
    "password": "<YOUR PASSWORD>",
    "schema": "<YOUR SCHEMA>",
    "extra": {
        "account": "<YOUR ACCOUNT>",
        "database": "<YOUR DATABASE>",
        "region": "<YOUR REGION>",
        "warehouse": "<YOUR WAREHOUSE>"
    }
}
```

### Stitch

```
{
    "conn_type": "http",
    "host": "https://api.stitchdata.com/v4",
    "extra": {
        "Content-Type": "application/json",
        "Authorization": "Bearer <YOUR STITCH API KEY>"
    }
}
```

You will also need your Source ID and Client ID to configure the operator.

### Postgres

```
{
    "conn_type": "postgres",
    "login": "<YOUR LOGIN>",
    "password": "<YOUR PASSWORD>",
    "schema": "<YOUR SCHEMA>",
    "host": "<YOUR HOST>"
}
```

###

This operator relies on a user having setup PynamoDB and configured Dynamo models locally. Please consult the PynamoDB documentation for further information: `https://pynamodb.readthedocs.io/en/stable/`

## Importing

To import any of these addons, simply update your `requirements.txt` file in your airflow directory to include the following:

```
vivian-airflow-extensions @ git+https://github.com/nursefly/vivian-airflow-extensions@<MOST RECENT COMMIT ID>
```

## Operators

### SnowflakeToDynamoOperator

This operator fetches data from a Snowflake table and inserts it into a DynamoDB table. It uses the `ExtendedSnowflakeHook` to connect to Snowflake and execute the SQL query, and the `DynamoDBHook` to connect to DynamoDB and insert the data.

The operator works as follows:

1. It establishes a connection to Snowflake using the `ExtendedSnowflakeHook`.
2. It executes the SQL query to fetch data from the Snowflake table.
3. The fetched data is then formatted into a format that DynamoDB can accept.
4. It establishes a connection to DynamoDB using the `DynamoDBHook`.
5. It inserts the formatted data into the DynamoDB table.

#### SnowflakeToDynamoBookmarkOperator

The `SnowflakeToDynamoOperator` can also be configured to use a bookmark to keep track of the last processed record. This is useful in scenarios where you want to incrementally load data from Snowflake to DynamoDB.

The bookmark functionality works as follows:

1. Before fetching data from the Snowflake table, the operator checks if there's a bookmark stored in an S3 bucket using the `S3BookmarkHook`.
2. If a bookmark exists, the operator uses it to modify the SQL query to only fetch records that are newer than the bookmark.
3. After the data is successfully inserted into the DynamoDB table, the operator updates the bookmark in the S3 bucket with the timestamp of the last processed record.

To use the bookmark functionality, you need to use the `SnowflakeToDynamoBookmarkOperator` instead of the `SnowflakeToDynamoOperator`.

### SnowflakeToPostgresOperator

This operator fetches data from a Snowflake table and inserts it into a PostgreSQL table. It uses the `ExtendedSnowflakeHook` to connect to Snowflake and execute the SQL query, and the `PostgresHook` to connect to PostgreSQL and insert the data.

The operator works as follows:

1. It establishes a connection to Snowflake using the `ExtendedSnowflakeHook`.
2. It executes the SQL query to fetch data from the Snowflake table.
3. The fetched data is then formatted into a format that PostgreSQL can accept.
4. It establishes a connection to PostgreSQL using the `PostgresHook`.
5. It inserts the formatted data into the PostgreSQL table.

#### SnowflakeToPostgresMergeIncrementalOperator

This operator fetches incremental data from a Snowflake table, merges it with existing data in a PostgreSQL table, and updates the PostgreSQL table with the merged data. It uses the `ExtendedSnowflakeHook` to connect to Snowflake and execute the SQL query, and the `PostgresHook` to connect to PostgreSQL and insert the data.

The operator works as follows:

1. It establishes a connection to Snowflake using the `ExtendedSnowflakeHook`.
2. It executes the SQL query to fetch incremental data from the Snowflake table.
3. The fetched data is then formatted into a format that PostgreSQL can accept.
4. It establishes a connection to PostgreSQL using the `PostgresHook`.
5. It merges the fetched data with the existing data in the PostgreSQL table.
6. It updates the PostgreSQL table with the merged data.

#### SnowflakeToPostgresBookmarkOperator

The `SnowflakeToPostgresBookmarkOperator` can also be configured to use a bookmark to keep track of the last processed record. This is useful in scenarios where you want to incrementally load and merge data from Snowflake to PostgreSQL.

The bookmark functionality works as follows:

1. Before fetching data from the Snowflake table, the operator checks if there's a bookmark stored in an S3 bucket using the `S3BookmarkHook`.
2. If a bookmark exists, the operator uses it to modify the SQL query to only fetch records that are newer than the bookmark.
3. After the data is successfully merged and updated in the PostgreSQL table, the operator updates the bookmark in the S3 bucket with the timestamp of the last processed record.

To use the bookmark functionality, you need to use the `SnowflakeToPostgresBookmarkOperator` instead of the `SnowflakeToPostgresOperator`.

### StitchRunSourceOperator

This operator triggers a run of a Stitch source. It uses the `StitchHook` to connect to Stitch and trigger the run.

The operator works as follows:

1. It establishes a connection to Stitch using the `StitchHook`.
2. It triggers a run of the specified Stitch source.

#### StitchRunAndMonitorSourceOperator

This operator triggers a run of a Stitch source and monitors it until it completes. It uses the `StitchHook` to connect to Stitch, trigger the run, and check the status of the run.

The operator works as follows:

1. It establishes a connection to Stitch using the `StitchHook`.
2. It triggers a run of the specified Stitch source.
3. It continuously checks the status of the run until it completes. If the run fails, the operator will raise an exception.

## Sensors

### StitchSensor

This sensor checks the status of a Stitch source and waits until it completes. It uses the `StitchHook` to connect to Stitch and check the status of the source.

The sensor works as follows:

1. It establishes a connection to Stitch using the `StitchHook`.
2. It continuously checks the status of the specified Stitch source.
3. If the source is still running, the sensor will sleep for a specified interval and then check the status again.
4. The sensor completes when the Stitch source has finished running. If the source fails, the sensor will raise an exception.

## Hooks

### ExtendedSnowflakeHook

This hook extends the functionality of the built-in `SnowflakeHook` in Apache Airflow. It provides a connection to Snowflake, and allows you to execute SQL queries against a Snowflake database. It's used in the `SnowflakeToDynamoOperator`, `SnowflakeToDynamoBookmarkOperator`, `SnowflakeToPostgresOperator`, and `SnowflakeToPostgresBookmarkOperator`.

The hook works as follows:

1. It establishes a connection to Snowflake using the connection details provided.
2. It provides a method to execute SQL queries against the Snowflake database.

### S3BookmarkHook

This hook provides a connection to Amazon S3. It allows you to store and retrieve bookmarks, which are used to keep track of the last processed record in a data transfer operation. It's used in the `SnowflakeToDynamoBookmarkOperator` and `SnowflakeToPostgresBookmarkOperator`.

The hook works as follows:

1. It establishes a connection to Amazon S3 using the connection details provided.
2. It provides methods to store and retrieve bookmarks from an S3 bucket.

### ExtendedPostgresHook

This hook provides a connection to PostgreSQL. It allows you to insert data into a PostgreSQL table. It's used in the `SnowflakeToPostgresOperator` and `SnowflakeToPostgresBookmarkOperator`.

The hook works as follows:

1. It establishes a connection to PostgreSQL using the connection details provided.
2. It provides a method to insert data into a PostgreSQL table.

### StitchHook

This hook provides a connection to Stitch. It allows you to trigger a run of a Stitch source and check the status of a run. It's used in the `StitchRunSourceOperator`, `StitchRunAndMonitorSourceOperator`, and `StitchSensor`.

The hook works as follows:

1. It establishes a connection to Stitch using the connection details provided.
2. It provides methods to trigger a run of a Stitch source and check the status of a run.

