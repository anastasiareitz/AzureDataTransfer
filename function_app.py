import hashlib
import importlib.util
import json
import logging
import math
import os
import random
import string
import time
import uuid
from dataclasses import dataclass
from io import BytesIO, StringIO
from typing import Annotated

import azure.functions as func
import pandas as pd
from azure.data.tables import TableClient, UpdateMode
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.storage.blob import ContainerClient
from azure.storage.queue import QueueClient, QueueMessage
from fastapi import FastAPI, HTTPException, Body, Header, Query
from pydantic import BaseModel, Field

# pyarrow required for pandas parquet output, check without importing
check_if_packages_installed = ["pyarrow"]
for each_package in check_if_packages_installed:
    if not importlib.util.find_spec(each_package):
        raise Exception(f"Failed to import package: {each_package}")

# fastapi settings for Azure Functions and API Management
# reference: https://blog.pamelafox.org/2022/11/fastapi-on-azure-functions-with-azure.html
# /api will require a subscription key
# /public/docs will not require a subscription key
fastapi_app = FastAPI(
    servers=[{"url": "/api", "description": "API"}],
    root_path="/public",
    root_path_in_servers=False,
    title="Azure Log Analytics Data Export API",
    swagger_ui_parameters={"defaultModelsExpandDepth": 0},
)

# azure auth via managed identity
# Azure Portal -> Function App -> Identity -> System Assigned
# Note: requires the following roles:
# 1. Monitoring Metrics Publisher
# 2. Log Analytics Contributor
# 3. Storage Queue Data Contributor
# 4. Storage Queue Data Message Processor
# 5. Storage Blob Data Contributor
# 6. Storage Table Data Contributor
credential = DefaultAzureCredential()

# setup for storage queue triggers via managed identity
# env variables (required)
# Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
# add 1. storageAccountConnectionString__queueServiceUri -> https://<STORAGE_ACCOUNT>.queue.core.windows.net/
# add 2. storageAccountConnectionString__credential -> managedidentity
# add 3. QueueQueryName -> <QUEUE_NAME>
# add 4. QueueProcessName -> <QUEUE_NAME>
# pylint: disable=invalid-name
env_var_queue_query_name = os.environ.get("QueueQueryName")
poison_queue_query_name = str(env_var_queue_query_name) + "-poison"
env_var_queue_process_name = os.environ.get("QueueProcessName")
poison_queue_process_name = str(env_var_queue_process_name) + "-poison"
# support for other law endpoints
# add 5. log_analytics_endpoint -> https://api.loganalytics.io/v1
env_var_law_endpoint = os.environ.get("LogAnalyticsEndpoint")
if not env_var_law_endpoint:
    env_var_law_endpoint = "https://api.loganalytics.io/v1"
# pylint: enable=invalid-name

# additional env variables to simplify requests (optional)
env_var_storage_queue_url = os.environ.get("QueueURL")
env_var_storage_table_url = os.environ.get("TableURL")
env_var_storage_table_ingest_name = os.environ.get("TableIngestName")
env_var_storage_table_query_name = os.environ.get("TableQueryName")
env_var_storage_table_process_name = os.environ.get("TableProcessName")


# -----------------------------------------------------------------------------
# log analytics ingest
# -----------------------------------------------------------------------------


def break_up_ingest_requests(
    start_datetime: str,
    time_delta_seconds: float,
    number_of_rows: int,
    max_rows_per_request: int,
) -> pd.DataFrame:
    number_of_loops = math.ceil(number_of_rows / max_rows_per_request)
    next_start_datetime = pd.to_datetime(start_datetime)
    rows_to_generate = number_of_rows
    ingest_requests = []
    for _ in range(number_of_loops):
        # start datetimes
        each_ingest_request = {}
        each_next_start_datetime = next_start_datetime.strftime("%Y-%m-%d %H:%M:%S.%f")
        each_ingest_request["start_datetime"] = each_next_start_datetime
        # determine number of rows for each request
        if rows_to_generate < max_rows_per_request:
            request_number_of_rows = rows_to_generate
        else:
            request_number_of_rows = max_rows_per_request
        each_ingest_request["number_of_rows"] = request_number_of_rows
        ingest_requests.append(each_ingest_request)
        # update number of rows and start datetime for next request
        rows_to_generate -= request_number_of_rows
        next_start_datetime += pd.to_timedelta(
            request_number_of_rows * time_delta_seconds, unit="s"
        )
    ingest_requests_df = pd.DataFrame(ingest_requests)
    return ingest_requests_df


def generate_test_data(
    start_date: str,
    timedelta_seconds: int,
    number_of_rows: int,
    number_of_columns: int,
    random_length: int = 10,
) -> pd.DataFrame:
    # create dataframe
    start_datetime = pd.to_datetime(start_date)
    timedelta = pd.Series(range(number_of_rows)) * pd.to_timedelta(
        f"{timedelta_seconds}s"
    )
    fake_time_column = start_datetime + timedelta
    fake_data_df = pd.DataFrame(
        {
            "TimeGenerated": fake_time_column,
        }
    )
    for each_index in range(1, number_of_columns + 1):
        each_column_name = f"DataColumn{each_index}"
        each_column_value = "".join(
            random.choice(string.ascii_lowercase) for i in range(random_length)
        )
        fake_data_df[each_column_name] = each_column_value
    # convert datetime to string column to avoid issues in log analytics
    time_generated = fake_data_df["TimeGenerated"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    fake_data_df["TimeGenerated"] = time_generated
    # status
    logging.info("Data Shape: %s", fake_data_df.shape)
    size_calculation = fake_data_df.memory_usage().sum() / 1_000_000
    logging.info("Size: %s MBs", size_calculation)
    get_first_datetime = fake_data_df["TimeGenerated"].iloc[0]
    get_last_datetime = fake_data_df["TimeGenerated"].iloc[-1]
    logging.info("First Datetime: %s", get_first_datetime)
    logging.info("Last Datetime: %s", get_last_datetime)
    return fake_data_df


def log_analytics_ingest(
    fake_data_df: pd.DataFrame,
    ingest_client: LogsIngestionClient,
    rule_id: str,
    stream_name: str,
) -> int:
    try:
        # convert to json
        body = json.loads(fake_data_df.to_json(orient="records", date_format="iso"))
        # send to log analytics
        ingest_client.upload(rule_id=rule_id, stream_name=stream_name, logs=body)
        logging.info("Send Successful")
        # return count of rows
        return fake_data_df.shape[0]
    except Exception as e:
        logging.error("Error sending to log analytics, will skip: %s", e)
        return 0


def generate_and_ingest_test_data(
    credential: DefaultAzureCredential,
    endpoint: str,
    rule_id: str,
    stream_name: str,
    storage_table_url: str,
    storage_table_ingest_name: str,
    start_date: str,
    timedelta_seconds: float,
    number_of_rows: int,
    number_of_columns: int = 10,
    max_rows_per_request=5_000_000,
) -> dict:
    """
    Generates test/fake data and ingests in Log Analytics
        note: credential requires Log Analytics Contributor and Monitor Publisher roles
        note: 10M rows with 10 columns takes about 15-20 minutes
    Log Analytics Data Collection Endpoint and Rule setup:
        1. azure portal -> monitor -> create data collection endpoint
        2. azure portal -> log analytics -> tables -> create new custom table in log analytics
        3. create data collection rule and add publisher role permissions
        reference: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal
    Args:
        credential: DefaultAzureCredential
        endpoint: log analytics endpoint url
            format: "https://{name}-XXXX.eastus-1.ingest.monitor.azure.com"
        rule_id: required log analytics ingestion param
            format: "dcr-XXXXXXXXXXXXXXXXXXXXXXXXXXXX"
        stream_name: required log analytics ingestion param
            format: "Custom-{tablename}"
        storage_table_url: url for storage table
            format: "https://{storage_account_name}.table.core.windows.net/"
        storage_table_ingest_name: name of storage table for ingest logs
        start_date: date to insert fake data
            format: YYYY-MM-DD HH:MM:SS
            note: can only ingest dates up to 2 days in the past and 1 day into the future
            reference: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/log-standard-columns
        timedelta_seconds: time between each fake data row
        number_of_rows: total number of rows to generate
        number_of_columns: total number of columns to generate
            note: for new columns, you need to update the schema before ingestion
            1. azure portal -> log analytics -> settings - tables -> ... -> edit schema
            2. azure portal -> data collection rules -> export template -> deploy -> edit
        max_rows_per_request: limit on number of rows to generate for each ingest
            note: lower this if running out memory
            note: 5M rows with 10 columns requires about 4-8 GB of RAM
    Returns:
        dict with results summary
    """
    time_start = time.time()
    # input validation
    given_timestamp = pd.to_datetime(start_date)
    current_datetime = pd.to_datetime("today")
    check_start_range = current_datetime - pd.to_timedelta("2D")
    check_end_range = current_datetime + pd.to_timedelta("1D")
    if not check_start_range <= given_timestamp <= check_end_range:
        logging.warning("Warning: Date given is outside allowed ingestion range")
        logging.warning("Note: Log Analytics will use ingest time as TimeGenerated")
        valid_ingest_datetime_range = False
    else:
        valid_ingest_datetime_range = True
    if number_of_rows < 2 or number_of_columns < 2:
        raise Exception("invalid row and/or column numbers")
    # log analytics ingest connection
    ingest_client = LogsIngestionClient(endpoint, credential)
    # storage table connection for logging
    # note: requires Storage Table Data Contributor role
    table_client = TableClient(
        storage_table_url, storage_table_ingest_name, credential=credential
    )
    # break up ingests
    ingest_requests_df = break_up_ingest_requests(
        start_date, timedelta_seconds, number_of_rows, max_rows_per_request
    )
    number_of_ingests = len(ingest_requests_df)
    # loop through requests
    successfull_rows_sent = 0
    for each_row in ingest_requests_df.itertuples():
        each_index = each_row.Index + 1
        each_request_start_time = time.time()
        each_start_datetime = each_row.start_datetime
        each_number_of_rows = each_row.number_of_rows
        # generate fake data
        logging.info("Generating Request %s of %s...", each_index, number_of_ingests)
        try:
            each_fake_data_df = generate_test_data(
                each_start_datetime,
                timedelta_seconds,
                each_number_of_rows,
                number_of_columns,
            )
        except Exception as e:
            logging.error("Unable to generate test data: %s", e)
            continue
        # send to log analytics
        logging.info("Sending to Log Analytics...")
        each_rows_ingested = log_analytics_ingest(
            each_fake_data_df,
            ingest_client,
            rule_id,
            stream_name,
        )
        successfull_rows_sent += each_rows_ingested
        runtime_calculation = round(time.time() - each_request_start_time, 1)
        logging.info("Runtime: %s seconds", runtime_calculation)
    # status check
    if successfull_rows_sent == 0:
        status = "Failed"
    elif successfull_rows_sent == number_of_rows:
        status = "Success"
    else:
        status = "Partial"
    # create partition key and row key
    ingest_uuid = str(uuid.uuid4())
    first_datetime = pd.to_datetime(start_date).strftime("%Y-%m-%d %H:%M:%S.%f")
    last_datetime = each_fake_data_df["TimeGenerated"].iloc[-1]
    row_key = f"{ingest_uuid}__{status}__"
    row_key += f"{first_datetime}__{last_datetime}__{timedelta_seconds}__"
    row_key += f"{number_of_columns}__{number_of_rows}__{successfull_rows_sent}"
    unique_row_sha256_hash = hashlib.sha256(row_key.encode()).hexdigest()
    # response and logging to table storage
    runtime = round(time.time() - time_start, 1)
    time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
    return_message = {
        "PartitionKey": ingest_uuid,
        "RowKey": unique_row_sha256_hash,
        "Status": status,
        "StartDatetime": first_datetime,
        "EndDatetime": last_datetime,
        "TimeDeltaSeconds": timedelta_seconds,
        "NumberColumns": number_of_columns,
        "RowsGenerated": number_of_rows,
        "RowsIngested": successfull_rows_sent,
        "ValidDatetimeRange": valid_ingest_datetime_range,
        "RuntimeSeconds": runtime,
        "TimeGenerated": time_generated,
    }
    table_client.upsert_entity(return_message, mode=UpdateMode.REPLACE)
    return return_message


# -----------------------------------------------------------------------------
# log analytics query
# -----------------------------------------------------------------------------


def query_log_analytics_request(
    workspace_id: str,
    log_client: LogsQueryClient,
    kql_query: str,
    request_wait_seconds: float = 0.05,
) -> pd.DataFrame:
    """
    Makes API query request to log analytics
    limits: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/api/timeouts
    API query limits:
        500,000 rows per request
        200 requests per 30 seconds
        max query time is 10 min
        100MB data max per request
    """
    try:
        # query log analytics
        response = log_client.query_workspace(
            workspace_id=workspace_id,
            query=kql_query,
            timespan=None,
            server_timeout=600,
        )
        # convert to dataframe
        if response.status == LogsQueryStatus.SUCCESS:
            table = response.tables[0]
            df = pd.DataFrame(data=table.rows, columns=table.columns)
            return df
        elif response.status == LogsQueryStatus.PARTIAL:
            raise Exception(
                f"Unsuccessful Request, Response Status: {response.status} {response.partial_error}"
            )
        else:
            raise Exception(
                f"Unsuccessful Request, Response Status: {response.status} {response}"
            )
    except Exception as e:
        raise Exception("Failed Log Analytics Request") from e
    finally:
        time.sleep(request_wait_seconds)


def query_log_analytics_connection_request(
    credential: DefaultAzureCredential, workspace_id: str, kql_query: str
) -> pd.DataFrame:
    # log analytics connection
    # note: need to add Log Analytics Contributor and Monitor Publisher role
    log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
    # submit query request
    result_df = query_log_analytics_request(workspace_id, log_client, kql_query)
    return result_df


def query_log_analytics_get_table_columns(
    table_names_and_columns: dict,
    workspace_id: str,
    log_client: LogsQueryClient,
) -> dict:
    output = {}
    for each_table, each_columns in table_names_and_columns.items():
        # column names provided
        if each_columns:
            each_columns_fix = each_columns
            if "TimeGenerated" not in each_columns:
                each_columns_fix = ["TimeGenerated"] + each_columns
            output[each_table] = each_columns_fix
            # test table and columns
            try:
                columns_to_project = ", ".join(each_columns_fix)
                each_kql_query = f"""
                let TABLE_NAME = "{each_table}";
                table(TABLE_NAME)
                | project {columns_to_project}
                | take 1
                """
                each_df = query_log_analytics_request(
                    workspace_id, log_client, each_kql_query
                )
            except Exception as e:
                exception_output = f"Invalid Table and/or Column Names: {each_table} - {columns_to_project}"
                logging.info(exception_output)
                raise Exception(exception_output) from e
        # if no column names provided, query log analytics for all column names
        else:
            logging.info("Getting columns names for %s", each_table)
            # add hidden column(s)
            try:
                each_kql_query = f"""
                let TABLE_NAME = "{each_table}";
                table(TABLE_NAME)
                | extend _ItemId
                | take 1
                """
                each_df = query_log_analytics_request(
                    workspace_id, log_client, each_kql_query
                )
            # don't add hidden column(s)
            except Exception:
                each_kql_query = f"""
                let TABLE_NAME = "{each_table}";
                table(TABLE_NAME)
                | take 1
                """
                each_df = query_log_analytics_request(
                    workspace_id, log_client, each_kql_query
                )
            each_columns_fix = list(each_df.columns)
            # confirm TimeGenerate exists in table
            if "TimeGenerated" not in each_columns_fix:
                exception_output = (
                    f"TimeGenerated not in table {each_table} - {each_columns_fix}"
                )
                logging.info(exception_output)
                raise Exception(exception_output)
            # reorder columns, places the following columns at begining of output
            columns_to_reorder = ["TimeGenerated", "_ItemId"]
            for each_col in columns_to_reorder[::-1]:
                if each_col in each_columns_fix:
                    each_columns_fix.remove(each_col)
                    each_columns_fix = [each_col] + each_columns_fix
            logging.info("Columns Detected: %s - %s", each_table, each_columns_fix)
            output[each_table] = each_columns_fix
    if len(output) == 0:
        logging.info("No valid table names")
        raise Exception("No valid table names")
    return output


def break_up_initial_date_range(
    table_name: str, start_datetime: str, end_datetime: str, freq: str
) -> pd.DataFrame:
    date_range = pd.date_range(start=start_datetime, end=end_datetime, freq=freq)
    if date_range[-1] != pd.to_datetime(end_datetime):
        date_range = date_range.union(pd.to_datetime([end_datetime]))
    date_ranges = [each.strftime("%Y-%m-%d %H:%M:%S.%f") for each in date_range.to_list()]
    time_pairs = [
        (date_ranges[i], date_ranges[i + 1]) for i in range(len(date_ranges) - 1)
    ]
    # convert to dataframe
    df_time_pairs = pd.DataFrame(time_pairs, columns=["start_date", "end_date"])
    df_time_pairs.insert(loc=0, column="table", value=[table_name] * len(df_time_pairs))
    return df_time_pairs


def break_up_initial_query_time_freq(
    table_names: list[str], start_datetime: str, end_datetime: str, freq: str
) -> pd.DataFrame:
    results = []
    # break up by table names
    for each_table_name in table_names:
        # break up date ranges by day
        each_df = break_up_initial_date_range(
            each_table_name, start_datetime, end_datetime, freq
        )
        results.append(each_df)
    df_results = pd.concat(results)
    return df_results


def query_log_analytics_get_time_ranges(
    workspace_id: str,
    log_client: LogsQueryClient,
    table_name: str,
    start_datetime: str,
    end_datetime: str,
    query_row_limit: int,
) -> pd.DataFrame:
    # converted KQL output to string columns to avoid datetime digits getting truncated
    kql_query = f"""
    let TABLE_NAME = "{table_name}";
    let START_DATETIME = datetime({start_datetime});
    let END_DATETIME = datetime({end_datetime});
    let QUERY_ROW_LIMIT = {query_row_limit};
    let table_datetime_filtered = table(TABLE_NAME)
    | project TimeGenerated
    | where (TimeGenerated >= START_DATETIME) and (TimeGenerated < END_DATETIME);
    let table_size = toscalar(
    table_datetime_filtered
    | count);
    let time_splits = table_datetime_filtered
    | order by TimeGenerated asc
    | extend row_index = row_number()
    | where row_index == 1 or row_index % (QUERY_ROW_LIMIT) == 0 or row_index == table_size;
    let time_pairs = time_splits
    | project StartTime = TimeGenerated
    | extend EndTime = next(StartTime)
    | where isnotnull(EndTime)
    | extend StartTime = tostring(StartTime), EndTime = tostring(EndTime);
    time_pairs
    """
    logging.info("Splitting %s: %s-%s", table_name, start_datetime, end_datetime)
    # query log analytics and get time ranges
    df = query_log_analytics_request(workspace_id, log_client, kql_query)
    # no results
    if df.shape[0] == 0:
        return pd.DataFrame()
    # datetime fix for events on final datetime
    # using copy and .loc to prevent chaining warning
    df_copy = df.copy()
    final_endtime = df_copy["EndTime"].tail(1).item()
    new_final_endtime = str(pd.to_datetime(final_endtime) + pd.to_timedelta("0.0000001s"))
    new_final_endtime_fix_format = new_final_endtime.replace(" ", "T")
    new_final_endtime_fix_format = new_final_endtime_fix_format.replace("00+00:00", "Z")
    df_copy.loc[df_copy.index[-1], "EndTime"] = new_final_endtime_fix_format
    return df_copy


def query_log_analytics_get_table_count(
    workspace_id: str,
    log_client: LogsQueryClient,
    table_name: str,
    start_datetime: str,
    end_datetime: str,
) -> int:
    kql_query = f"""
    let TABLE_NAME = "{table_name}";
    let START_DATETIME = datetime({start_datetime});
    let END_DATETIME = datetime({end_datetime});
    table(TABLE_NAME)
    | project TimeGenerated
    | where (TimeGenerated >= START_DATETIME) and (TimeGenerated < END_DATETIME)
    | count
    """
    df = query_log_analytics_request(workspace_id, log_client, kql_query)
    return df.values[0][0]


def query_log_analytics_add_table_row_counts(
    input_df: pd.DataFrame,
    workspace_id: str,
    log_client: LogsQueryClient,
    table_name: str,
) -> pd.DataFrame:
    # add row counts
    results = []
    for each_row in input_df.itertuples():
        each_starttime = each_row.StartTime
        each_endtime = each_row.EndTime
        each_count = query_log_analytics_get_table_count(
            workspace_id, log_client, table_name, each_starttime, each_endtime
        )
        results.append(each_count)
    input_df["Count"] = results
    return input_df


def query_log_analytics_split_query_rows(
    workspace_id: str,
    log_client: LogsQueryClient,
    table_name: str,
    start_datetime: str,
    end_datetime: str,
    query_row_limit: int,
    query_row_limit_correction: int,
) -> pd.DataFrame:
    # fix for large number of events at same datetime
    query_row_limit_fix = query_row_limit - query_row_limit_correction
    # get time ranges
    results_df = query_log_analytics_get_time_ranges(
        workspace_id,
        log_client,
        table_name,
        start_datetime,
        end_datetime,
        query_row_limit_fix,
    )
    # empty results
    if results_df.shape[0] == 0:
        return pd.DataFrame()
    # add row counts column
    results_df = query_log_analytics_add_table_row_counts(
        results_df, workspace_id, log_client, table_name
    )
    # warning if query limit exceeded, change limits and try again
    if results_df.Count.gt(query_row_limit).any():
        raise Exception(f"Sub-Query exceeds query row limit, {list(results_df.Count)}")
    # add table name column
    results_df.insert(loc=0, column="Table", value=[table_name] * len(results_df))
    return results_df


def query_log_analytics_split_query_rows_loop(
    df_queries: pd.DataFrame,
    workspace_id: str,
    log_client: LogsQueryClient,
    query_row_limit: int,
    query_row_limit_correction: int,
) -> pd.DataFrame:
    logging.info("Querying Log Analytics to Split Query...")
    query_results = []
    for each_query in df_queries.itertuples():
        each_table_name = each_query.table
        each_start_datetime = each_query.start_date
        each_end_datetime = each_query.end_date
        each_results_df = query_log_analytics_split_query_rows(
            workspace_id,
            log_client,
            each_table_name,
            each_start_datetime,
            each_end_datetime,
            query_row_limit,
            query_row_limit_correction,
        )
        query_results.append(each_results_df)
        # each status
        each_status = f"Completed {each_table_name}: "
        each_status += f"{each_start_datetime}-{each_end_datetime} "
        each_status += f"-> {each_results_df.shape[0]} Queries"
        logging.info(each_status)
    # combine all results
    results_df = pd.concat(query_results)
    return results_df


def process_query_results_df(
    query_results_df: pd.DataFrame,
    query_uuid: str,
    table_names_and_columns: dict,
    subscription_id: str,
    resource_group: str,
    worksapce_name: str,
    workspace_id: str,
    storage_blob_url: str,
    storage_blob_name: str,
    storage_blob_output: str,
    storage_table_url: str,
    storage_table_name: str,
) -> list[dict]:
    # add column names
    column_names = query_results_df["Table"].apply(lambda x: table_names_and_columns[x])
    query_results_df.insert(loc=1, column="Columns", value=column_names)
    # add azure property columns
    query_results_df.insert(loc=0, column="QueryUUID", value=query_uuid)
    query_results_df.insert(loc=5, column="Subscription", value=subscription_id)
    query_results_df.insert(loc=6, column="ResourceGroup", value=resource_group)
    query_results_df.insert(loc=7, column="LogAnalyticsWorkspace", value=worksapce_name)
    query_results_df.insert(loc=8, column="LogAnalyticsWorkspaceId", value=workspace_id)
    query_results_df.insert(loc=9, column="StorageBlobURL", value=storage_blob_url)
    query_results_df.insert(loc=10, column="StorageContainer", value=storage_blob_name)
    query_results_df.insert(loc=11, column="OutputFormat", value=storage_blob_output)
    query_results_df.insert(loc=12, column="StorageTableURL", value=storage_table_url)
    query_results_df.insert(loc=13, column="StorageTableName", value=storage_table_name)
    # rename columns
    query_results_df_rename = query_results_df.rename(
        columns={"StartTime": "StartDatetime", "EndTime": "EndDatetime"}
    )
    # convert to dictionary
    results = query_results_df_rename.to_dict(orient="records")
    return results


def query_log_analytics_send_to_queue(
    query_uuid: str,
    credential: DefaultAzureCredential,
    subscription_id: str,
    resource_group: str,
    worksapce_name: str,
    workspace_id: str,
    storage_queue_url: str,
    storage_queue_name: str,
    storage_blob_url: str,
    storage_blob_container: str,
    storage_table_url: str,
    storage_table_query_name: str,
    storage_table_process_name: str,
    table_names_and_columns: dict,
    start_datetime: str,
    end_datetime: str,
    query_row_limit: int = 250_000,
    query_row_limit_correction: int = 1_000,
    break_up_query_freq="4h",
    storage_blob_output_format: str = "JSONL",
) -> dict:
    """
    Splits query date range into smaller queries and sends to storage queue
        note: credential requires Log Analytics, Storage Queue, and Table Storage Contributor roles
        note: date range is processed as [start_datetime, end_datetime)
    Args:
        query_uuid: uuid for full query
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        credential: azure default credential object
        subscription_id: azure subscription id
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        resource_group: azure resource group
        workspace_name: name of log analytics workspace
        workspace_id: log analytics workspace id
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        storage_queue_url: storage account queue url
            format: "https://{storage_account_name}.queue.core.windows.net/"
        storage_queue_name: name of storage queue
        storage_blob_url: storage blob account url to save output
            format: https://{storage_account_name}.blob.core.windows.net/"
        storage_blob_container: name of container in storage account to save output
        storage_table_url: storage table url
            format: "https://{storage_account_name}.table.core.windows.net/"
        storage_table_query_name: name of storage table for query logs
        storage_table_process_name: name of storage table for process logs
        table_names_and_columns: dictionary of table names with columns to project
            note: blank column list will detect and use all columns
            format:  {"table_name" : ["column_1", "column_2", ... ], ... }
        start_datetime: starting datetime, inclusive
            format: YYYY-MM-DD HH:MM:SS
        start_datetime: ending datetime, exclusive
            format: YYYY-MM-DD HH:MM:SS
        query_row_limit: max number of rows for each follow-up query/message
        query_row_limit_correction: correction factor in case of overlapping data
        break_up_query_freq: limit on query datetime range to prevent crashes
            note:for  more than 10M rows per hour, use 4 hours or less
        storage_blob_output_format: output file format, options = "JSONL", "CSV", "PARQUET"
            note: JSONL is json line delimited
    Return
        dict of results summary
    """
    start_time = time.time()
    # input validation
    try:
        pd.to_datetime(start_datetime)
        pd.to_datetime(end_datetime)
    except Exception as e:
        raise Exception("Invalid Datetime Format") from e
    if storage_blob_output_format not in ["JSONL", "CSV", "PARQUET"]:
        raise Exception(f"Invalid Output file format: {storage_blob_output_format}")
    # status message
    logging.info("Processing Query...")
    table_names_join = ", ".join(table_names_and_columns.keys())
    logging.info("Tables: %s", table_names_join)
    logging.info("Date Range: %s-%s", start_datetime, end_datetime)
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
    # storage queue connection
    # note: need to add Storage Queue Data Contributor role
    storage_queue_url_and_name = storage_queue_url + storage_queue_name
    queue_client = QueueClient.from_queue_url(storage_queue_url_and_name, credential)
    # storage table connection for logging
    # note: requires Storage Table Data Contributor role
    table_client = TableClient(
        storage_table_url, storage_table_query_name, credential=credential
    )
    # process table and column names
    table_names_and_columns = query_log_analytics_get_table_columns(
        table_names_and_columns, workspace_id, log_client
    )
    # get expected count of full queries
    total_query_results_count_expected = 0
    for each_table_name in table_names_and_columns:
        each_count = query_log_analytics_get_table_count(
            workspace_id, log_client, each_table_name, start_datetime, end_datetime
        )
        total_query_results_count_expected += each_count
    logging.info("Total Row Count: %s", total_query_results_count_expected)
    if total_query_results_count_expected > 0:
        # break up queries by table and date ranges
        table_names = list(table_names_and_columns.keys())
        df_queries = break_up_initial_query_time_freq(
            table_names, start_datetime, end_datetime, break_up_query_freq
        )
        # query log analytics, gets datetime splits for row limit
        query_results_df = query_log_analytics_split_query_rows_loop(
            df_queries,
            workspace_id,
            log_client,
            query_row_limit,
            query_row_limit_correction,
        )
        if not query_results_df.empty:
            # confirm count of split queries
            total_query_results_count = query_results_df["Count"].sum()
            logging.info("Split Queries Total Row Count: %s", total_query_results_count)
            if total_query_results_count != total_query_results_count_expected:
                raise Exception("Error: Row Count Mismatch")
            # process results, add columns, and convert to list of dicts
            results = process_query_results_df(
                query_results_df,
                query_uuid,
                table_names_and_columns,
                subscription_id,
                resource_group,
                worksapce_name,
                workspace_id,
                storage_blob_url,
                storage_blob_container,
                storage_blob_output_format,
                storage_table_url,
                storage_table_process_name,
            )
            number_of_results = len(results)
            # send to queue
            successful_sends = 0
            get_queue_properties = queue_client.get_queue_properties()
            logging.info("Initial Queue Status: %s", get_queue_properties)
            for each_msg in results:
                each_result = send_message_to_queue(queue_client, each_msg)
                if each_result == "Success":
                    successful_sends += 1
            logging.info("Messages Successfully Sent to Queue: %s", successful_sends)
            get_queue_properties = queue_client.get_queue_properties()
            logging.info("Updated Queue Status: %s", get_queue_properties)
            if successful_sends == number_of_results:
                status = "Success"
            else:
                status = "Partial"
        # no results
        else:
            status = "Failed"
            total_query_results_count = 0
            number_of_results = 0
            successful_sends = 0
            logging.error("Error: No Query Messages Generated, No Query Results")
    # no data during datetime range
    else:
        status = "Success"
        total_query_results_count = 0
        number_of_results = 0
        successful_sends = 0
        logging.info("No data during %s-%s", start_datetime, end_datetime)
    # create hash for RowKey
    row_key = f"{query_uuid}__{status}__{table_names_join}__"
    row_key += f"{start_datetime}__{end_datetime}__"
    row_key += f"{total_query_results_count}__{number_of_results}__{successful_sends}"
    unique_row_sha256_hash = hashlib.sha256(row_key.encode()).hexdigest()
    # response and logging to table storage
    runtime = round(time.time() - start_time, 1)
    time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
    return_message = {
        "PartitionKey": query_uuid,
        "RowKey": unique_row_sha256_hash,
        "Status": status,
        "Tables": table_names_join,
        "StartDatetime": start_datetime,
        "EndDatetime": end_datetime,
        "TotalRowCount": int(total_query_results_count),
        "MessagesGenerated": number_of_results,
        "MessagesSentToQueue": successful_sends,
        "RuntimeSeconds": runtime,
        "TimeGenerated": time_generated,
    }
    table_client.upsert_entity(return_message, mode=UpdateMode.REPLACE)
    return return_message


# -----------------------------------------------------------------------------
# storage queue
# -----------------------------------------------------------------------------


def send_message_to_queue(
    queue_client: QueueClient, message: str, request_wait_seconds: float = 0.05
) -> str:
    try:
        queue_client.send_message(json.dumps(message))
        return "Success"
    except Exception as e:
        logging.error(
            "Error: Unable to send message to queue, skipped: %s, exception: %s",
            message,
            e,
        )
        return "Failed"
    finally:
        time.sleep(request_wait_seconds)


def get_message_from_queue(
    queue_client: QueueClient,
    message_visibility_timeout_seconds: int,
    request_wait_seconds: float = 0.05,
) -> QueueMessage | None:
    # queue calls have built-in 10x retry policy
    # ref: https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/storage/azure-storage-queue#optional-configuration
    try:
        queue_message = queue_client.receive_message(
            visibility_timeout=message_visibility_timeout_seconds
        )
        return queue_message
    except Exception as e:
        logging.error("Request Error: Unable to Get Queue Message, %s", e)
        raise Exception("Request Error: Unable to Get Queue Message") from e
    finally:
        time.sleep(request_wait_seconds)


def delete_message_from_queue(
    queue_client: QueueClient, queue_message: QueueMessage
) -> None:
    try:
        queue_client.delete_message(queue_message)
        logging.info("Successfully Deleted Message from Queue")
    except Exception as e:
        logging.error("Unable to delete message, %s, %s", queue_message, e)
        raise Exception(f"Unable to delete message, {queue_message}") from e


def check_if_queue_empty_peek_message(queue_client: QueueClient) -> bool:
    try:
        peek_messages = queue_client.peek_messages()
        if not peek_messages:
            return True
        return False
    except Exception as e:
        logging.error("Unable to peek at queue messages, %s", e)
        return False


def query_message_validation_check(message: dict) -> None:
    required_fields = [
        "QueryUUID",
        "Subscription",
        "ResourceGroup",
        "LogAnalyticsWorkspace",
        "LogAnalyticsWorkspaceId",
        "StorageQueueURL",
        "StorageQueueName",
        "StorageBlobURL",
        "StorageContainer",
        "StorageTableURL",
        "StorageTableQueryName",
        "StorageTableProcessName",
        "TableNamesColumns",
        "StartDatetime",
        "EndDatetime",
        "QueryRowLimit",
        "QueryRowLimitCorrection",
        "BreakUpQueryFreq",
        "StorageBlobOutputFormat",
    ]
    if not all(each_field in message for each_field in required_fields):
        logging.error("Invalid message, required fields missing: %s", message)
        raise Exception(f"Invalid message, required fields missing: {message}")


def process_message_validation_check(message: dict) -> None:
    required_fields = [
        "QueryUUID",
        "Table",
        "Columns",
        "StartDatetime",
        "EndDatetime",
        "Subscription",
        "ResourceGroup",
        "LogAnalyticsWorkspace",
        "LogAnalyticsWorkspaceId",
        "StorageBlobURL",
        "StorageContainer",
        "OutputFormat",
        "StorageTableURL",
        "StorageTableName",
        "Count",
    ]
    if not all(each_field in message for each_field in required_fields):
        logging.error("Invalid message, required fields missing: %s", message)
        raise Exception(f"Invalid message, required fields missing: {message}")


def query_log_analytics_get_query_results(
    log_client: LogsQueryClient, message: dict
) -> pd.DataFrame:
    # extract message fields
    workspace_id = message["LogAnalyticsWorkspaceId"]
    table_name = message["Table"]
    column_names = message["Columns"]
    start_datetime = message["StartDatetime"]
    end_datetime = message["EndDatetime"]
    # query log analytics
    columns_to_project = ", ".join(column_names)
    kql_query = f"""
    let TABLE_NAME = "{table_name}";
    let START_DATETIME = datetime({start_datetime});
    let END_DATETIME = datetime({end_datetime});
    table(TABLE_NAME)
    | project {columns_to_project}
    | where (TimeGenerated >= START_DATETIME) and (TimeGenerated < END_DATETIME)
    """
    df = query_log_analytics_request(workspace_id, log_client, kql_query)
    return df


def datetime_to_filename_safe(user_input: str) -> str:
    # remove characters from timestamp to be filename safe/readable
    output = user_input.replace("-", "").replace(":", "").replace(".", "")
    output = output.replace("T", "").replace("Z", "")
    output = output.replace(" ", "")
    return output


def generate_output_filename_base(
    message: str,
    output_filename_timestamp: pd.Timestamp,
) -> str:
    # extract message
    table_name = message["Table"]
    subscription = message["Subscription"]
    resource_group = message["ResourceGroup"]
    log_analytics_name = message["LogAnalyticsWorkspace"]
    start_datetime = message["StartDatetime"]
    end_datetime = message["EndDatetime"]
    # datetime conversion via pandas: dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # extract datetime values for filename
    extract_year = output_filename_timestamp.strftime("%Y")
    extract_month = output_filename_timestamp.strftime("%m")
    extract_day = output_filename_timestamp.strftime("%d")
    extract_hour = output_filename_timestamp.strftime("%H")
    # mimics continuous export from log analytics
    # https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-data-export
    output_filename = f"{table_name}/"
    output_filename += "WorkspaceResourceId=/"
    output_filename += f"subscriptions/{subscription}/"
    output_filename += f"resourcegroups/{resource_group}/"
    output_filename += "providers/microsoft.operationalinsights/"
    output_filename += f"workspaces/{log_analytics_name}/"
    output_filename += f"y={extract_year}/m={extract_month}/d={extract_day}/"
    output_filename += f"h={extract_hour}/"
    output_filename += f"{datetime_to_filename_safe(start_datetime)}-"
    output_filename += f"{datetime_to_filename_safe(end_datetime)}"
    return output_filename


def output_filename_and_format(
    results_df: pd.DataFrame, output_format: str, output_filename_base: str
) -> tuple[bytes | str]:
    # file format
    output_filename = output_filename_base
    if output_format == "JSONL":
        output_filename += ".json"
        output_data = results_df.to_json(
            orient="records", lines=True, date_format="iso", date_unit="ns"
        )
        return output_filename, output_data
    elif output_format == "CSV":
        output_filename += ".csv"
        output_data = results_df.to_csv(index=False)
        return output_filename, output_data
    elif output_format == "PARQUET":
        output_filename += ".parquet"
        output_data = results_df.to_parquet(index=False, engine="pyarrow")
        return output_filename, output_data


def process_queue_message(
    log_client: LogsQueryClient,
    message: dict,
) -> None:
    """
    Processes individual message: validates, queries log analytics, and saves results to storage account
    Args:
        log_client: azure log analytics LogsQueryClient object
        message: message content dictionary
    Returns:
        None
    """
    start_time = time.time()
    # validate message
    process_message_validation_check(message)
    logging.info("Processing Message: %s", message)
    # query log analytics
    query_results_df = query_log_analytics_get_query_results(log_client, message)
    logging.info("Successfully Downloaded from Log Analytics: %s", query_results_df.shape)
    # confirm count matches
    if query_results_df.shape[0] != message["Count"]:
        logging.error("Row count doesn't match expected value, %s", message)
        raise Exception(f"Row count doesn't match expected value, {message}")
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    storage_blob_url = message["StorageBlobURL"]
    storage_container_name = message["StorageContainer"]
    container_client = ContainerClient(
        storage_blob_url, storage_container_name, credential
    )
    # storage table connection for logging
    # note: requires Storage Table Data Contributor role
    storage_table_url = message["StorageTableURL"]
    storage_table_name = message["StorageTableName"]
    table_client = TableClient(
        storage_table_url, storage_table_name, credential=credential
    )
    # output filename and file format
    output_format = message["OutputFormat"]
    output_filename_timestamp = query_results_df["TimeGenerated"].iloc[0]
    output_filename_base = generate_output_filename_base(
        message, output_filename_timestamp
    )
    full_output_filename, output_data = output_filename_and_format(
        query_results_df, output_format, output_filename_base
    )
    # upload to blob storage
    file_size = upload_file_to_storage(
        container_client, full_output_filename, output_data
    )
    status = "Success"
    # logging success to storage table
    query_uuid = message["QueryUUID"]
    table_name = message["Table"]
    start_datetime = message["StartDatetime"]
    start_datetime = start_datetime.replace("T", " ").replace("Z", "")
    end_datetime = message["EndDatetime"]
    end_datetime = end_datetime.replace("T", " ").replace("Z", "")
    row_count = message["Count"]
    # generate unique row key
    row_key = f"{query_uuid}__{status}__{table_name}__"
    row_key += f"{start_datetime}__{end_datetime}__{row_count}__"
    row_key += f"{full_output_filename}__{file_size}"
    unique_row_sha256_hash = hashlib.sha256(row_key.encode()).hexdigest()
    # response and logging to storage table
    runtime_seconds = round(time.time() - start_time, 1)
    time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
    return_message = {
        "PartitionKey": query_uuid,
        "RowKey": unique_row_sha256_hash,
        "Status": status,
        "Table": table_name,
        "StartDatetime": start_datetime,
        "EndDatetime": end_datetime,
        "RowCount": row_count,
        "Filename": full_output_filename,
        "FileSizeBytes": file_size,
        "RuntimeSeconds": runtime_seconds,
        "TimeGenerated": time_generated,
    }
    table_client.upsert_entity(return_message, mode=UpdateMode.REPLACE)


def process_queue_messages_loop(
    credential: DefaultAzureCredential,
    storage_queue_url: str,
    storage_queue_name: str,
    message_visibility_timeout_seconds: int = 600,
) -> dict:
    """
    Processes Log Analytics query jobs/messages from a storage queue and exports to Blob Storage
        note: credential requires Log Analytics Contributor, Storage Queue Data Contributor, and Storage Blob Data Contributor roles
        note: takes ~150 seconds for a query with 500k rows and 10 columns to csv (100 seconds for parquet)
        note: intended to be run interactively, for example, in a notebook or terminal
        note: for production environment, use an azure function app
    Args:
        credential: azure default credential object
        storage_queue_url: storage account queue url
            format: "https://{storage_account_name}.queue.core.windows.net/"
        storage_queue_name: name of queue
        message_visibility_timeout_seconds: number of seconds for queue message visibility
    Returns:
        dict of results summary
    """
    logging.info("Processing Queue Messages, press CTRL+C or interupt kernel to stop...")
    start_time = time.time()
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
    # storage queue connection
    # note: need to add Storage Queue Data Contributor role
    storage_queue_url_and_name = storage_queue_url + storage_queue_name
    queue_client = QueueClient.from_queue_url(storage_queue_url_and_name, credential)
    # process messages from queue until empty
    successful_messages = 0
    failed_messages = 0
    try:
        # loop through all messages in queue
        while True:
            # queue status
            get_queue_properties = queue_client.get_queue_properties()
            logging.info("Queue Status: %s", get_queue_properties)
            # get message
            each_start_time = time.time()
            queue_message = get_message_from_queue(
                queue_client, message_visibility_timeout_seconds
            )
            if queue_message:
                try:
                    # extract content
                    message_content = json.loads(queue_message.content)
                    # process message: validate, query log analytics, upload to storage
                    process_queue_message(log_client, message_content)
                    # remove message from queue if successful
                    delete_message_from_queue(queue_client, queue_message)
                    successful_messages += 1
                    runtime_calculation = round(time.time() - each_start_time, 1)
                    logging.info("Runtime: %s", runtime_calculation)
                except Exception as e:
                    logging.error(
                        "Unable to process message: %s %s", queue_message.content, e
                    )
                    failed_messages += 1
                    continue
            # queue empty
            else:
                logging.info(
                    "Waiting for message visibility timeout (%s seconds)...",
                    message_visibility_timeout_seconds,
                )
                time.sleep(message_visibility_timeout_seconds + 60)
                # check if queue still empty
                if check_if_queue_empty_peek_message(queue_client):
                    logging.info("No messages in queue")
                    break
    # stop processing by keyboard interrupt
    except KeyboardInterrupt:
        logging.warning("Run was canceled manually by user")
    # return results
    finally:
        get_queue_properties = queue_client.get_queue_properties()
        logging.info("Queue Status: %s", get_queue_properties)
        logging.info("Processing queue messages complete")
        return_result = {
            "successful_messages": successful_messages,
            "failed_messages": failed_messages,
            "runtime_seconds": round(time.time() - start_time, 1),
        }
        return return_result  # pylint: disable=return-in-finally disable=lost-exception


# -----------------------------------------------------------------------------
# storage blob
# -----------------------------------------------------------------------------


def upload_file_to_storage(
    container_client: ContainerClient,
    filename: str,
    data: bytes | str,
    azure_storage_connection_timeout_fix_seconds: int = 600,
) -> int:
    # note: need to use undocumented param connection_timeout to avoid timeout errors
    # ref: https://stackoverflow.com/questions/65092741/solve-timeout-errors-on-file-uploads-with-new-azure-storage-blob-package
    try:
        blob_client = container_client.get_blob_client(filename)
        blob_client.upload_blob(
            data=data,
            connection_timeout=azure_storage_connection_timeout_fix_seconds,
            overwrite=True,
        )
        storage_account_name = container_client.account_name
        container_name = container_client.container_name
        logging.info(
            "Successfully Uploaded %s:%s/%s",
            storage_account_name,
            container_name,
            filename,
        )
        # file size
        uploaded_file_metadata = list(container_client.list_blobs(filename))[0]
        uploaded_file_size = uploaded_file_metadata.size
        file_size_calculation_mb = uploaded_file_size / 1_000_000
        logging.info("File Size: %s MBs", file_size_calculation_mb)
        return uploaded_file_size
    except Exception as e:
        logging.error("Unable to upload, %s, %s", filename, e)
        raise Exception(f"Unable to upload, {filename}") from e


def download_blob(
    filename: str,
    credential: DefaultAzureCredential,
    storage_blob_url: str,
    storage_container_name: str,
) -> pd.DataFrame:
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    container_client = ContainerClient(
        storage_blob_url, storage_container_name, credential
    )
    # download data
    blob_client = container_client.get_blob_client(filename)
    downloaded_blob = blob_client.download_blob()
    if filename.endswith(".json"):
        stream = StringIO(downloaded_blob.content_as_text())
        output_df = pd.read_json(stream, lines=True)
    elif filename.endswith(".csv"):
        stream = StringIO(downloaded_blob.content_as_text())
        output_df = pd.read_csv(stream)
    elif filename.endswith(".parquet"):
        stream = BytesIO()
        downloaded_blob.readinto(stream)
        output_df = pd.read_parquet(stream, engine="pyarrow")
    else:
        raise Exception("file extension not supported")
    return output_df


def list_blobs_df(
    credential: DefaultAzureCredential,
    storage_blob_url: str,
    storage_container_name: str,
) -> pd.DataFrame:
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    container_client = ContainerClient(
        storage_blob_url, storage_container_name, credential
    )
    # get blobs
    results = []
    for each_file in container_client.list_blobs():
        each_name = each_file.name
        each_size_mb = each_file.size / 1_000_000
        each_date = each_file.creation_time
        results.append([each_name, each_size_mb, each_date])
    # convert to dataframe
    df = pd.DataFrame(results, columns=["filename", "file_size_mb", "creation_time"])
    df = df.sort_values("creation_time", ascending=False)
    return df


# -----------------------------------------------------------------------------
# storage table
# -----------------------------------------------------------------------------


def get_and_process_table_results(
    credential: DefaultAzureCredential,
    storage_table_url: str,
    storage_table_query_name: str,
    storage_table_process_name: str,
    query_uuid: str,
) -> dict[str, pd.DataFrame]:
    cols_to_rename = {"PartitionKey": "QueryUUID"}
    return_dfs = {
        "query_results_df": pd.DataFrame(),
        "process_results_df": pd.DataFrame(),
        "success_process_results_df": pd.DataFrame(),
        "failed_process_results_df": pd.DataFrame(),
    }
    # table connections
    table_client_query = TableClient(
        storage_table_url, storage_table_query_name, credential=credential
    )
    table_client_process = TableClient(
        storage_table_url, storage_table_process_name, credential=credential
    )
    # get results from azure storage tables
    search_odata_string = f"PartitionKey eq '{query_uuid}'"
    query_results = table_client_query.query_entities(search_odata_string)
    process_results = table_client_process.query_entities(search_odata_string)
    # query results
    query_results_df_raw = pd.DataFrame(query_results)
    if query_results_df_raw.shape[0] == 0:
        logging.info("Query UUID %s not found in query logs", query_uuid)
        return return_dfs
    else:
        query_results_df = query_results_df_raw.rename(columns=cols_to_rename)
        return_dfs["query_results_df"] = query_results_df
    # process results
    process_results_df_raw = pd.DataFrame(process_results)
    if process_results_df_raw.shape[0] == 0:
        logging.info("Query UUID %s not found in process logs", query_uuid)
        return return_dfs
    else:
        process_results_df = process_results_df_raw.rename(columns=cols_to_rename)
        return_dfs["process_results_df"] = process_results_df
        # split
        success_mask = process_results_df.Status == "Success"
        failed_mask = process_results_df.Status == "Failed"
        success_process_results_df = process_results_df.loc[success_mask]
        failed_process_results_df = process_results_df.loc[failed_mask]
        return_dfs["success_process_results_df"] = success_process_results_df
        return_dfs["failed_process_results_df"] = failed_process_results_df
        return return_dfs


def calculate_runtime_since_query_submit(
    query_results_df: pd.DataFrame, process_results_df: pd.DataFrame
) -> int:
    # change column types to datetime
    query_results_df_copy = query_results_df.copy()
    query_results_df_copy["TimeGenerated"] = pd.to_datetime(
        query_results_df.TimeGenerated
    )
    process_results_df_copy = process_results_df.copy()
    process_results_df_copy["TimeGenerated"] = pd.to_datetime(
        process_results_df_copy.TimeGenerated
    )
    # calculate difference between first submit and last processed log
    query_submit_datetime = query_results_df_copy["TimeGenerated"].min()
    last_processing_datetime = process_results_df_copy["TimeGenerated"].max()
    time_since_query = last_processing_datetime - query_submit_datetime
    time_since_query_seconds = time_since_query.total_seconds()
    return time_since_query_seconds


def calculate_processing_status_and_percent(
    number_of_successful_subqueries: int,
    number_of_subqueries: int,
    total_success_row_count: int,
    query_total_row_count: int,
) -> tuple[str, float]:
    if (
        number_of_successful_subqueries == number_of_subqueries
        and total_success_row_count == query_total_row_count
    ):
        processing_status = "Complete"
    else:
        processing_status = "Partial"
    percent_complete = (number_of_successful_subqueries / number_of_subqueries) * 100
    percent_complete = round(percent_complete, 1)
    return processing_status, percent_complete


def calculate_file_size(
    filesize_units: str,
    total_success_bytes: int,
) -> tuple[float, str]:
    if filesize_units == "GB":
        divisor = 1_000_000_000
        success_total_size = float(round(total_success_bytes / divisor, 3))
        file_units = "GB"
    elif filesize_units == "TB":
        divisor = 1_000_000_000_000
        success_total_size = float(round(total_success_bytes / divisor, 3))
        file_units = "TB"
    # defaults to MB
    else:
        divisor = 1_000_000
        success_total_size = float(round(total_success_bytes / divisor, 3))
        file_units = "MB"
    return success_total_size, file_units


def calculate_time_remaining_estimate(
    processing_status: str,
    number_of_successful_subqueries: int,
    percent_complete: float,
    time_since_query_seconds: int,
) -> float | None:
    if processing_status == "Complete":
        time_remaining_seconds = 0.0
    else:
        if number_of_successful_subqueries > 0:
            percent_remaining = 100 - percent_complete
            time_remaining_seconds = (
                time_since_query_seconds * percent_remaining / percent_complete
            )
            time_remaining_seconds = float(round(time_remaining_seconds, 1))
        else:
            time_remaining_seconds = None
    return time_remaining_seconds


def get_status(
    credential: DefaultAzureCredential,
    query_uuid: str,
    storage_table_url: str,
    storage_table_query_name: str,
    storage_table_process_name: str,
    return_failures: bool = True,
    filesize_units: str = "GB",
) -> dict:
    """
    Gets status of submitted query
    Args:
        query_uuid: query uuid or "PartitionKey"
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        storage_table_url: storage table url
            format: "https://{storage_account_name}.table.core.windows.net/"
        storage_table_query_name: name of storage table for query logs
        storage_table_process_name: name of storage table for process logs
        return_failures: will return details on failed jobs/messages
        filesize_units: "MB", "GB", or "TB"
    Returns:
        dict with high-level status properties
    """
    # get table logs dataframes
    table_dfs = get_and_process_table_results(
        credential,
        storage_table_url,
        storage_table_query_name,
        storage_table_process_name,
        query_uuid,
    )
    query_results_df = table_dfs["query_results_df"]
    process_results_df = table_dfs["process_results_df"]
    success_process_results_df = table_dfs["success_process_results_df"]
    failed_process_results_df = table_dfs["failed_process_results_df"]
    # query results
    results = {"query_uuid": query_uuid}
    no_results_status = "No Records Found"
    if query_results_df.empty:
        results["query_submit_status"] = no_results_status
        results["query_processing_status"] = no_results_status
        return results
    if len(query_results_df.Status.unique()) == 1:
        query_results_status = str(query_results_df.Status.unique()[0])
    else:
        query_results_status = str(query_results_df.Status.value_counts().to_dict())
        query_results_status = query_results_status.replace("{", "").replace("}", "")
        query_results_status = query_results_status.replace("'", "")
    query_submit_status = query_results_status
    number_of_query_splits = query_results_df.shape[0]
    number_of_subqueries = query_results_df.MessagesSentToQueue.sum()
    query_total_row_count = query_results_df.TotalRowCount.sum()
    results["query_submit_status"] = query_submit_status
    results["query_submit_splits"] = number_of_query_splits
    results["number_of_subqueries"] = int(number_of_subqueries)
    results["query_total_row_count"] = int(query_total_row_count)
    # process results
    if process_results_df.empty:
        results["query_processing_status"] = no_results_status
        return results
    number_of_successful_subqueries = success_process_results_df.shape[0]
    number_of_failed_subqueries = failed_process_results_df.shape[0]
    total_success_bytes = success_process_results_df.FileSizeBytes.sum()
    total_success_row_count = success_process_results_df.RowCount.sum()
    time_since_query_seconds = calculate_runtime_since_query_submit(
        query_results_df, process_results_df
    )
    processing_status, percent_complete = calculate_processing_status_and_percent(
        number_of_successful_subqueries,
        number_of_subqueries,
        total_success_row_count,
        query_total_row_count,
    )
    success_total_size, file_units = calculate_file_size(
        filesize_units, total_success_bytes
    )
    time_remaining_seconds = calculate_time_remaining_estimate(
        processing_status,
        number_of_successful_subqueries,
        percent_complete,
        time_since_query_seconds,
    )
    results["query_processing_status"] = processing_status
    results["processing_percent_complete"] = float(percent_complete)
    results["number_of_subqueries_success"] = number_of_successful_subqueries
    results["number_of_subqueries_failed"] = number_of_failed_subqueries
    results["output_total_row_count"] = int(total_success_row_count)
    results["output_file_size"] = success_total_size
    results["output_file_units"] = file_units
    results["runtime_since_submit_seconds"] = round(time_since_query_seconds, 1)
    results["processing_estimated_time_remaining_seconds"] = time_remaining_seconds
    # failures
    if return_failures and failed_process_results_df.shape[0] > 0:
        export_cols = [
            "Table",
            "StartDatetime",
            "EndDatetime",
            "RowCount",
        ]
        rename_columns = {
            "Table": "table",
            "StartDatetime": "start_datetime",
            "EndDatetime": "end_datetime",
            "RowCount": "row_count",
        }
        export_df = failed_process_results_df[export_cols]
        export_df_renamed = export_df.rename(columns=rename_columns)
        results["failures"] = export_df_renamed.to_dict(orient="records")
    else:
        results["failures"] = [{}]
    return results


# -----------------------------------------------------------------------------
# Pydantic input/output validation for FastAPI HTTP requests
# -----------------------------------------------------------------------------


@dataclass
class RegEx:
    """regular expressions used by pydantic input validation"""

    uuid: str = (
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    )
    sub_key: str = r"^[0-9a-fA-F]{32}$"
    datetime: str = r"^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}"
    url: str = r"^(http|https)://"
    dcr: str = r"^dcr-"


class APIMExceptionOutput(BaseModel):
    """output for APIM Exceptions"""

    statusCode: str
    message: str


class HTTPExceptionOutput(BaseModel):
    """output for HTTP Exceptions"""

    detail: str


class IngestInput(BaseModel):
    """input validation for azure_ingest_data()"""

    log_analytics_data_collection_endpoint: str = Field(pattern=RegEx.url, min_length=10)
    log_analytics_data_collection_rule_id: str = Field(pattern=RegEx.dcr, min_length=5)
    log_analytics_data_collection_stream_name: str = Field(min_length=3)
    storage_table_url: str = Field(
        default=env_var_storage_table_url,
        pattern=RegEx.url,
        min_length=10,
        validate_default=True,
    )
    storage_table_ingest_name: str = Field(
        default=env_var_storage_table_ingest_name, min_length=3, validate_default=True
    )
    start_datetime: str = Field(pattern=RegEx.datetime)
    timedelta_seconds: float = Field(gt=0.0)
    number_of_rows: int = Field(gt=0)
    number_of_columns: int = Field(default=10, gt=2)
    max_rows_per_request: int = Field(default=5_000_000, gt=0)


class IngestOuput(BaseModel):
    """output validation for azure_ingest_data()"""

    ingest_uuid: str
    ingest_status: str
    table_stream_name: str
    start_datetime: str
    end_datetime: str
    number_of_columns: int
    rows_generated: int
    rows_ingested: int
    valid_datetime_range: bool
    runtime_seconds: float
    ingest_datetime: str


class SubmitQueryInput(BaseModel):
    """input validation for azure_submit_query()"""

    query_uuid: str = Field(default=None, pattern=RegEx.uuid, validate_default=False)
    subscription_id: str = Field(pattern=RegEx.uuid)
    resource_group_name: str = Field(min_length=3)
    log_analytics_worksapce_name: str = Field(min_length=3)
    log_analytics_workspace_id: str = Field(pattern=RegEx.uuid)
    storage_queue_url: str = Field(
        default=env_var_storage_queue_url,
        pattern=RegEx.url,
        min_length=10,
        validate_default=True,
    )
    storage_queue_process_name: str = Field(
        default=env_var_queue_process_name, min_length=3, validate_default=True
    )
    storage_blob_url: str = Field(pattern=RegEx.url, min_length=10)
    storage_blob_container_name: str = Field(min_length=3)
    storage_table_url: str = Field(
        default=env_var_storage_table_url,
        pattern=RegEx.url,
        min_length=10,
        validate_default=True,
    )
    storage_table_query_name: str = Field(
        default=env_var_storage_table_query_name, min_length=3, validate_default=True
    )
    storage_table_process_name: str = Field(
        default=env_var_storage_table_process_name, min_length=3, validate_default=True
    )
    table_names_and_columns: dict[str, list[str]] = Field(min_length=1)
    start_datetime: str = Field(pattern=RegEx.datetime)
    end_datetime: str = Field(pattern=RegEx.datetime)
    query_row_limit: int = Field(default=250_000, gt=0)
    query_row_limit_correction: int = Field(default=1_000, ge=0)
    break_up_query_freq: str = Field(default="4h", min_length=2)
    storage_blob_output_format: str = Field(default="JSONL", min_length=3)


class SubmitQueryOutput(BaseModel):
    """output validation for azure_submit_query()"""

    query_uuid: str
    submit_status: str
    table_names: str
    start_datetime: str
    end_datetime: str
    total_row_count: int
    subqueries_generated: int
    subqueries_sent_to_queue: int
    runtime_seconds: float
    submit_datetime: str


class SubmitQueryParallelInput(SubmitQueryInput):
    """input validation for azure_submit_queries()"""

    parallel_process_break_up_query_freq: str = Field(default="1d", min_length=2)
    storage_queue_query_name: str = Field(
        default=env_var_queue_query_name, min_length=3, validate_default=True
    )


class SubmitQueryParallelOutput(BaseModel):
    """output validation for azure_submit_queries()"""

    query_uuid: str
    split_status: str
    table_names: str
    start_datetime: str
    end_datetime: str
    number_of_messages_generated: int
    number_of_messages_sent: int
    total_row_count: int
    runtime_seconds: float
    split_datetime: str


class GetQueryStatusInput(BaseModel):
    """input validation for azure_get_status_post()"""

    query_uuid: str = Field(pattern=RegEx.uuid)
    storage_table_url: str = Field(
        default=env_var_storage_table_url,
        pattern=RegEx.url,
        min_length=10,
        validate_default=True,
    )
    storage_table_query_name: str = Field(
        default=env_var_storage_table_query_name, min_length=3, validate_default=True
    )
    storage_table_process_name: str = Field(
        default=env_var_storage_table_process_name, min_length=3, validate_default=True
    )
    return_failures: bool = Field(default=True)
    filesize_units: str = Field(default="GB", min_length=2)


class GetQueryStatusOutput(BaseModel):
    """output validation for azure_get_status() and azure_get_status_post()"""

    query_uuid: str
    query_partitions: int
    submit_status: str
    processing_status: str
    percent_complete: float
    runtime_since_submit_seconds: float
    estimated_time_remaining_seconds: float
    number_of_subqueries: int
    number_of_subqueries_success: int
    number_of_subqueries_failed: int
    query_row_count: int
    output_row_count: int
    output_file_size: float
    output_file_units: str
    failures: list[dict]


class GetQueryStatusOutputNoQuery(BaseModel):
    """output validation for azure_get_status() and azure_get_status_post()"""

    query_uuid: str
    submit_status: str
    processing_status: str


class GetQueryStatusOutputNoProcess(BaseModel):
    """output validation for azure_get_status() and azure_get_status_post()"""

    query_uuid: str
    submit_status: str
    processing_status: str
    query_partitions: int
    number_of_subqueries: int
    query_row_count: int


class TestLawInput(BaseModel):
    """input validation for azure_test_law()"""

    workspace_id: str
    kql_query: str


class TestLawOutput(BaseModel):
    """output validation for azure_test_law()"""

    response: str
    results: str


# --------------------------------------------------------------------------------------
# Azure Functions - FastAPI HTTP Endpoints
# --------------------------------------------------------------------------------------


@fastapi_app.post(
    path="/azure_ingest_test_data",
    name="Ingest Test Data",
    responses={
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_ingest_test_data(
    validated_inputs: Annotated[
        IngestInput,
        Body(
            openapi_examples={
                "required": {
                    "summary": "Required Parameters",
                    "description": "Example using required parameters",
                    "value": {
                        "log_analytics_data_collection_endpoint": "https://<DATA_COLLECTION_ENDPOINT>.ingest.monitor.azure.com",
                        "log_analytics_data_collection_rule_id": "dcr-<RULE_ID>",
                        "log_analytics_data_collection_stream_name": "Custom-<TABLE_NAME>_CL",
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "timedelta_seconds": 0.000_1,
                        "number_of_rows": 1_000,
                    },
                },
                "optional": {
                    "summary": "Optional Parameters",
                    "description": "Example using optional parameters",
                    "value": {
                        "log_analytics_data_collection_endpoint": "https://<DATA_COLLECTION_ENDPOINT>.ingest.monitor.azure.com",
                        "log_analytics_data_collection_rule_id": "dcr-<RULE_ID>",
                        "log_analytics_data_collection_stream_name": "Custom-<TABLE_NAME>_CL",
                        "storage_table_url": "https://<STORAGE_ACCOUNT_NAME>.table.core.windows.net/",
                        "storage_table_ingest_name": "ingestlog",
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "timedelta_seconds": 0.000_1,
                        "number_of_rows": 1_000,
                        "number_of_columns": 10,
                        "max_rows_per_request": 5_000_000,
                    },
                },
            }
        ),
    ],
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> IngestOuput:
    """
    Ingest test data to log analytics
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_ingest_test_data function...")
    # extract fields
    endpoint = validated_inputs.log_analytics_data_collection_endpoint
    rule_id = validated_inputs.log_analytics_data_collection_rule_id
    stream_name = validated_inputs.log_analytics_data_collection_stream_name
    storage_table_url = validated_inputs.storage_table_url
    storage_table_ingest_name = validated_inputs.storage_table_ingest_name
    start_datetime = validated_inputs.start_datetime
    timedelta_seconds = validated_inputs.timedelta_seconds
    number_of_rows = validated_inputs.number_of_rows
    number_of_columns = validated_inputs.number_of_columns
    max_rows_per_request = validated_inputs.max_rows_per_request
    # generate fake data and ingest
    try:
        results = generate_and_ingest_test_data(
            credential,
            endpoint,
            rule_id,
            stream_name,
            storage_table_url,
            storage_table_ingest_name,
            start_datetime,
            timedelta_seconds,
            number_of_rows,
            number_of_columns=number_of_columns,
            max_rows_per_request=max_rows_per_request,
        )
        logging.info("Success: %s", results)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e
    # response
    return_response = {
        "ingest_uuid": results["PartitionKey"],
        "ingest_status": results["Status"],
        "table_stream_name": stream_name,
        "start_datetime": results["StartDatetime"],
        "end_datetime": results["EndDatetime"],
        "number_of_columns": results["NumberColumns"],
        "rows_generated": results["RowsGenerated"],
        "rows_ingested": results["RowsIngested"],
        "valid_datetime_range": results["ValidDatetimeRange"],
        "runtime_seconds": results["RuntimeSeconds"],
        "ingest_datetime": results["TimeGenerated"],
    }
    logging.info(return_response)
    return return_response


@fastapi_app.post(
    path="/azure_submit_query",
    name="Submit Query",
    responses={
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_submit_query(
    validated_inputs: Annotated[
        SubmitQueryInput,
        Body(
            openapi_examples={
                "required": {
                    "summary": "Required Parameters",
                    "description": "Example using required parameters",
                    "value": {
                        "subscription_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "resource_group_name": "<RESOURCE_GROUP_NAME>",
                        "log_analytics_worksapce_name": "<LOG_ANALYTICS_WORKSPACE_NAME>",
                        "log_analytics_workspace_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "storage_blob_url": "https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/",
                        "storage_blob_container_name": "<CONTAINER_NAME>",
                        "table_names_and_columns": {
                            "<TABLE_NAME>_CL": [
                                "TimeGenerated",
                                "<COLUMN_NAME_1>",
                                "<COLUMN_NAME_2>",
                                "<COLUMN_NAME_3>",
                            ]
                        },
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "end_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                    },
                },
                "optional": {
                    "summary": "Optional Parameters",
                    "description": "Example using optional parameters",
                    "value": {
                        "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "subscription_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "resource_group_name": "<RESOURCE_GROUP_NAME>",
                        "log_analytics_worksapce_name": "<LOG_ANALYTICS_WORKSPACE_NAME>",
                        "log_analytics_workspace_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "storage_queue_url": "https://<STORAGE_ACCOUNT_NAME>.queue.core.windows.net/",
                        "storage_queue_process_name": "<STORAGE_QUEUE_PROCESS_NAME>",
                        "storage_blob_url": "https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/",
                        "storage_blob_container_name": "<CONTAINER_NAME>",
                        "storage_table_url": "https://<STORAGE_ACCOUNT_NAME>.table.core.windows.net/",
                        "storage_table_query_name": "querylog",
                        "storage_table_process_name": "processlog",
                        "table_names_and_columns": {
                            "<TABLE_NAME>_CL": [
                                "TimeGenerated",
                                "<COLUMN_NAME_1>",
                                "<COLUMN_NAME_2>",
                                "<COLUMN_NAME_3>",
                            ]
                        },
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "end_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "query_row_limit": 250_000,
                        "query_row_limit_correction": 1000,
                        "break_up_query_freq": "4h",
                        "storage_blob_output_format": "JSONL",
                    },
                },
            }
        ),
    ],
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> SubmitQueryOutput:
    """
    Submit query to log analytics
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_submit_query function...")
    # extract fields
    query_uuid = validated_inputs.query_uuid
    if not query_uuid:
        query_uuid = str(uuid.uuid4())
    subscription_id = validated_inputs.subscription_id
    resource_group_name = validated_inputs.resource_group_name
    log_analytics_worksapce_name = validated_inputs.log_analytics_worksapce_name
    log_analytics_workspace_id = validated_inputs.log_analytics_workspace_id
    storage_queue_url = validated_inputs.storage_queue_url
    storage_queue_name = validated_inputs.storage_queue_process_name
    storage_blob_url = validated_inputs.storage_blob_url
    storage_blob_container_name = validated_inputs.storage_blob_container_name
    storage_table_url = validated_inputs.storage_table_url
    storage_table_query_name = validated_inputs.storage_table_query_name
    storage_table_process_name = validated_inputs.storage_table_process_name
    table_names_and_columns = validated_inputs.table_names_and_columns
    start_datetime = validated_inputs.start_datetime
    end_datetime = validated_inputs.end_datetime
    query_row_limit = validated_inputs.query_row_limit
    query_row_limit_correction = validated_inputs.query_row_limit_correction
    break_up_query_freq = validated_inputs.break_up_query_freq
    storage_blob_output_format = validated_inputs.storage_blob_output_format
    # split query, generate messages, and send to queue
    try:
        results = query_log_analytics_send_to_queue(
            query_uuid,
            credential,
            subscription_id,
            resource_group_name,
            log_analytics_worksapce_name,
            log_analytics_workspace_id,
            storage_queue_url,
            storage_queue_name,
            storage_blob_url,
            storage_blob_container_name,
            storage_table_url,
            storage_table_query_name,
            storage_table_process_name,
            table_names_and_columns,
            start_datetime,
            end_datetime,
            query_row_limit=query_row_limit,
            query_row_limit_correction=query_row_limit_correction,
            break_up_query_freq=break_up_query_freq,
            storage_blob_output_format=storage_blob_output_format,
        )
        logging.info("Success: %s", results)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e
    # response
    return_response = {
        "query_uuid": results["PartitionKey"],
        "submit_status": results["Status"],
        "table_names": results["Tables"],
        "start_datetime": results["StartDatetime"],
        "end_datetime": results["EndDatetime"],
        "total_row_count": results["TotalRowCount"],
        "subqueries_generated": results["MessagesGenerated"],
        "subqueries_sent_to_queue": results["MessagesSentToQueue"],
        "runtime_seconds": results["RuntimeSeconds"],
        "submit_datetime": results["TimeGenerated"],
    }
    logging.info(return_response)
    return return_response


@fastapi_app.post(
    path="/azure_submit_query_parallel",
    name="Submit Query",
    responses={
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_submit_query_parallel(
    validated_inputs: Annotated[
        SubmitQueryParallelInput,
        Body(
            openapi_examples={
                "required": {
                    "summary": "Required Parameters",
                    "description": "Example using required parameters",
                    "value": {
                        "subscription_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "resource_group_name": "<RESOURCE_GROUP_NAME>",
                        "log_analytics_worksapce_name": "<LOG_ANALYTICS_WORKSPACE_NAME>",
                        "log_analytics_workspace_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "storage_blob_url": "https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/",
                        "storage_blob_container_name": "<CONTAINER_NAME>",
                        "table_names_and_columns": {
                            "<TABLE_NAME>_CL": [
                                "TimeGenerated",
                                "<COLUMN_NAME_1>",
                                "<COLUMN_NAME_2>",
                                "<COLUMN_NAME_3>",
                            ]
                        },
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "end_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                    },
                },
                "optional": {
                    "summary": "Optional Parameters",
                    "description": "Example using optional parameters",
                    "value": {
                        "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "subscription_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "resource_group_name": "<RESOURCE_GROUP_NAME>",
                        "log_analytics_worksapce_name": "<LOG_ANALYTICS_WORKSPACE_NAME>",
                        "log_analytics_workspace_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "storage_queue_url": "https://<STORAGE_ACCOUNT_NAME>.queue.core.windows.net/",
                        "storage_queue_query_name": "<STORAGE_QUEUE_QUERY_NAME>",
                        "storage_queue_process_name": "<STORAGE_QUEUE_PROCESS_NAME>",
                        "storage_blob_url": "https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/",
                        "storage_blob_container_name": "<CONTAINER_NAME>",
                        "storage_table_url": "https://<STORAGE_ACCOUNT_NAME>.table.core.windows.net/",
                        "storage_table_query_name": "querylog",
                        "storage_table_process_name": "processlog",
                        "table_names_and_columns": {
                            "<TABLE_NAME>_CL": [
                                "TimeGenerated",
                                "<COLUMN_NAME_1>",
                                "<COLUMN_NAME_2>",
                                "<COLUMN_NAME_3>",
                            ]
                        },
                        "start_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "end_datetime": "YYYY-MM-DD HH:MM:SS.SSSSSS",
                        "query_row_limit": 250_000,
                        "query_row_limit_correction": 1000,
                        "parallel_process_break_up_query_freq": "1d",
                        "break_up_query_freq": "4h",
                        "storage_blob_output_format": "JSONL",
                    },
                },
            }
        ),
    ],
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> SubmitQueryParallelOutput:
    """
    Submits query to log analytics, splits initial datetime range, and runs in parallel
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_submit_queries function...")
    start_time = time.time()
    # extract values
    query_uuid = validated_inputs.query_uuid
    if not query_uuid:
        query_uuid = str(uuid.uuid4())
    subscription_id = validated_inputs.subscription_id
    resource_group_name = validated_inputs.resource_group_name
    workspace_name = validated_inputs.log_analytics_worksapce_name
    workspace_id = validated_inputs.log_analytics_workspace_id
    storage_queue_url = validated_inputs.storage_queue_url
    storage_queue_query_name = validated_inputs.storage_queue_query_name
    storage_queue_process_name = validated_inputs.storage_queue_process_name
    storage_blob_url = validated_inputs.storage_blob_url
    storage_container_name = validated_inputs.storage_blob_container_name
    storage_table_url = validated_inputs.storage_table_url
    storage_table_query_name = validated_inputs.storage_table_query_name
    storage_table_process_name = validated_inputs.storage_table_process_name
    table_names_and_columns = validated_inputs.table_names_and_columns
    start_datetime = validated_inputs.start_datetime
    end_datetime = validated_inputs.end_datetime
    query_row_limit = validated_inputs.query_row_limit
    query_row_limit_correction = validated_inputs.query_row_limit_correction
    break_up_query_freq = validated_inputs.break_up_query_freq
    output_format = validated_inputs.storage_blob_output_format
    # connections
    try:
        # storage queue connection
        # note: need to add Storage Queue Data Contributor role
        storage_queue_url_and_name = storage_queue_url + storage_queue_query_name
        queue_client = QueueClient.from_queue_url(storage_queue_url_and_name, credential)
        # log analytics connection
        # note: need to add Log Analytics Contributor and Monitor Publisher role
        log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed Connection: {e}") from e
    # split query and send to queue
    try:
        # get total result count
        total_query_count = 0
        for each_table_name in table_names_and_columns:
            each_count = query_log_analytics_get_table_count(
                workspace_id, log_client, each_table_name, start_datetime, end_datetime
            )
            total_query_count += each_count
        logging.info("Total Row Count: %s", total_query_count)
        if total_query_count > 0:
            # split up datetime range
            freq = validated_inputs.parallel_process_break_up_query_freq
            date_range = pd.date_range(start=start_datetime, end=end_datetime, freq=freq)
            if date_range[-1] != pd.to_datetime(end_datetime):
                date_range = date_range.union(pd.to_datetime([end_datetime]))
            date_ranges = [
                each.strftime("%Y-%m-%d %H:%M:%S.%f") for each in date_range.to_list()
            ]
            time_pairs = [
                (date_ranges[i], date_ranges[i + 1]) for i in range(len(date_ranges) - 1)
            ]
            # generate messages
            messages = []
            for each_time_pair in time_pairs:
                each_start_datetime, each_end_datetime = each_time_pair
                each_message = {}
                each_message["QueryUUID"] = query_uuid
                each_message["Subscription"] = subscription_id
                each_message["ResourceGroup"] = resource_group_name
                each_message["LogAnalyticsWorkspace"] = workspace_name
                each_message["LogAnalyticsWorkspaceId"] = workspace_id
                each_message["StorageQueueURL"] = storage_queue_url
                each_message["StorageQueueName"] = storage_queue_process_name
                each_message["StorageBlobURL"] = storage_blob_url
                each_message["StorageContainer"] = storage_container_name
                each_message["StorageTableURL"] = storage_table_url
                each_message["StorageTableQueryName"] = storage_table_query_name
                each_message["StorageTableProcessName"] = storage_table_process_name
                each_message["TableNamesColumns"] = table_names_and_columns
                each_message["StartDatetime"] = each_start_datetime
                each_message["EndDatetime"] = each_end_datetime
                each_message["QueryRowLimit"] = query_row_limit
                each_message["QueryRowLimitCorrection"] = query_row_limit_correction
                each_message["BreakUpQueryFreq"] = break_up_query_freq
                each_message["StorageBlobOutputFormat"] = output_format
                messages.append(each_message)
            number_of_messages = len(messages)
            # send messages to queue
            successful_sends = 0
            get_queue_properties = queue_client.get_queue_properties()
            logging.info("Initial Queue Status: %s", get_queue_properties)
            for each_msg in messages:
                each_result = send_message_to_queue(queue_client, each_msg)
                if each_result == "Success":
                    successful_sends += 1
            logging.info("Messages Successfully Sent to Queue: %s", successful_sends)
            get_queue_properties = queue_client.get_queue_properties()
            logging.info("Updated Queue Status: %s", get_queue_properties)
            if successful_sends == 0:
                logging.error(
                    "Failed to send any messages to query queue, %s of %s",
                    successful_sends,
                    number_of_messages,
                )
                status = "Failed"
            elif successful_sends != number_of_messages:
                logging.error(
                    "Failed to send some messages to query queue, %s of %s",
                    successful_sends,
                    number_of_messages,
                )
                status = "Partial"
            else:
                logging.info("Success, sent all messages to queue")
                status = "Success"
        # no data during datetime range
        else:
            status = "Success"
            number_of_messages = 0
            successful_sends = 0
            logging.info("No data during %s-%s", start_datetime, end_datetime)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e
    # response
    table_names_join = ", ".join(table_names_and_columns.keys())
    time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
    time_calculation = round(time.time() - start_time, 1)
    return_response = {
        "query_uuid": query_uuid,
        "split_status": status,
        "table_names": table_names_join,
        "start_datetime": start_datetime,
        "end_datetime": end_datetime,
        "number_of_messages_generated": number_of_messages,
        "number_of_messages_sent": successful_sends,
        "total_row_count": int(total_query_count),
        "runtime_seconds": time_calculation,
        "split_datetime": time_generated,
    }
    logging.info(return_response)
    return return_response


@fastapi_app.get(
    path="/azure_get_status",
    name="Get Query Status",
    responses={
        400: {"model": HTTPExceptionOutput, "description": "Input Error"},
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_get_status(
    query_uuid: Annotated[str | None, Query(pattern=RegEx.uuid)] = None,
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> GetQueryStatusOutput | GetQueryStatusOutputNoProcess | GetQueryStatusOutputNoQuery:
    """
    Get query status
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_get_status function...")
    # confirm uuid
    if not query_uuid:
        logging.error("Failed: no query uuid")
        raise HTTPException(status_code=400, detail="Failed: no query uuid")
    # confirm env variables
    env_vars = {
        "storage_table_url": env_var_storage_table_url,
        "storage_table_query_name": env_var_storage_table_query_name,
        "storage_table_process_name": env_var_storage_table_process_name,
    }
    for each_name, each_value in env_vars.items():
        if not each_value:
            logging.error("Failed: env variable %s not set", each_name)
            raise HTTPException(
                status_code=400, detail=f"Failed: env variable {each_name} not set"
            )
    # get status
    try:
        results = get_status(
            credential,
            query_uuid,
            env_var_storage_table_url,
            env_var_storage_table_query_name,
            env_var_storage_table_process_name,
        )
        logging.info("Success: %s", results)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e
    # response
    return_response = {
        "query_uuid": results["query_uuid"],
        "submit_status": results["query_submit_status"],
        "processing_status": results["query_processing_status"],
    }
    # no query records
    if results["query_submit_status"] == "No Records Found":
        logging.info("Results: %s", return_response)
        return return_response
    return_response["query_partitions"] = results["query_submit_splits"]
    return_response["number_of_subqueries"] = results["number_of_subqueries"]
    return_response["query_row_count"] = results["query_total_row_count"]
    # no process records
    if results["query_processing_status"] == "No Records Found":
        logging.info("Results: %s", return_response)
        return return_response
    return_response["number_of_subqueries_success"] = results[
        "number_of_subqueries_success"
    ]
    return_response["number_of_subqueries_failed"] = results[
        "number_of_subqueries_failed"
    ]
    return_response["output_row_count"] = results["output_total_row_count"]
    return_response["output_file_size"] = results["output_file_size"]
    return_response["output_file_units"] = results["output_file_units"]
    return_response["percent_complete"] = results["processing_percent_complete"]
    return_response["runtime_since_submit_seconds"] = results[
        "runtime_since_submit_seconds"
    ]
    return_response["estimated_time_remaining_seconds"] = results[
        "processing_estimated_time_remaining_seconds"
    ]
    return_response["failures"] = results["failures"]
    # re-order json output
    key_order = [
        "query_uuid",
        "query_partitions",
        "submit_status",
        "processing_status",
        "percent_complete",
        "runtime_since_submit_seconds",
        "estimated_time_remaining_seconds",
        "number_of_subqueries",
        "number_of_subqueries_success",
        "number_of_subqueries_failed",
        "query_row_count",
        "output_row_count",
        "output_file_size",
        "output_file_units",
        "failures",
    ]
    return_response_order = {k: return_response[k] for k in key_order}
    logging.info("Results: %s", return_response_order)
    return return_response_order


@fastapi_app.post(
    path="/azure_get_status",
    name="Get Query Status",
    responses={
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_get_status_post(
    validated_inputs: Annotated[
        GetQueryStatusInput,
        Body(
            openapi_examples={
                "required": {
                    "summary": "Required Parameters",
                    "description": "Example using required parameters",
                    "value": {
                        "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                    },
                },
                "optional": {
                    "summary": "Optional Parameters",
                    "description": "Example using optional parameters",
                    "value": {
                        "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "storage_table_url": "https://<STORAGE_ACCOUNT_NAME>.table.core.windows.net/",
                        "storage_table_query_name": "querylog",
                        "storage_table_process_name": "processlog",
                        "return_failures": True,
                        "filesize_units": "GB",
                    },
                },
            }
        ),
    ],
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> GetQueryStatusOutput | GetQueryStatusOutputNoProcess | GetQueryStatusOutputNoQuery:
    """
    Get query status
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_get_status_post function...")
    # extract fields
    query_uuid = validated_inputs.query_uuid
    storage_table_url = validated_inputs.storage_table_url
    storage_table_query_name = validated_inputs.storage_table_query_name
    storage_table_process_name = validated_inputs.storage_table_process_name
    return_failures = validated_inputs.return_failures
    filesize_units = validated_inputs.filesize_units
    # get status
    try:
        results = get_status(
            credential,
            query_uuid,
            storage_table_url,
            storage_table_query_name,
            storage_table_process_name,
            return_failures=return_failures,
            filesize_units=filesize_units,
        )
        logging.info("Success: %s", results)
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e
    # response
    return_response = {
        "query_uuid": results["query_uuid"],
        "submit_status": results["query_submit_status"],
        "processing_status": results["query_processing_status"],
    }
    # no query records
    if results["query_submit_status"] == "No Records Found":
        logging.info("Results: %s", return_response)
        return return_response
    return_response["query_partitions"] = results["query_submit_splits"]
    return_response["number_of_subqueries"] = results["number_of_subqueries"]
    return_response["query_row_count"] = results["query_total_row_count"]
    # no process records
    if results["query_processing_status"] == "No Records Found":
        logging.info("Results: %s", return_response)
        return return_response
    return_response["number_of_subqueries_success"] = results[
        "number_of_subqueries_success"
    ]
    return_response["number_of_subqueries_failed"] = results[
        "number_of_subqueries_failed"
    ]
    return_response["output_row_count"] = results["output_total_row_count"]
    return_response["output_file_size"] = results["output_file_size"]
    return_response["output_file_units"] = results["output_file_units"]
    return_response["percent_complete"] = results["processing_percent_complete"]
    return_response["runtime_since_submit_seconds"] = results[
        "runtime_since_submit_seconds"
    ]
    return_response["estimated_time_remaining_seconds"] = results[
        "processing_estimated_time_remaining_seconds"
    ]
    return_response["failures"] = results["failures"]
    # re-order json output
    key_order = [
        "query_uuid",
        "query_partitions",
        "submit_status",
        "processing_status",
        "percent_complete",
        "runtime_since_submit_seconds",
        "estimated_time_remaining_seconds",
        "number_of_subqueries",
        "number_of_subqueries_success",
        "number_of_subqueries_failed",
        "query_row_count",
        "output_row_count",
        "output_file_size",
        "output_file_units",
        "failures",
    ]
    return_response_order = {k: return_response[k] for k in key_order}
    logging.info("Results: %s", return_response_order)
    return return_response_order


@fastapi_app.post(
    path="/azure_test_law",
    name="Test Log Analytics Workspace Connection",
    responses={
        401: {"model": APIMExceptionOutput, "description": "Access Denied"},
        500: {"model": HTTPExceptionOutput, "description": "Server Error"},
    },
)
async def azure_test_law(
    validated_inputs: Annotated[
        TestLawInput,
        Body(
            openapi_examples={
                "required": {
                    "summary": "Required Parameters",
                    "description": "Example using required parameters",
                    "value": {
                        "workspace_id": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
                        "endpoint": "https://api.loganalytics.io/v1",
                        "kql_query": "TableName | take 1",
                    },
                },
            }
        ),
    ],
    # pylint: disable=unused-argument
    subscription_key: str | None = Header(
        default=None,
        title="API Management Subscription Key",
        alias="Ocp-Apim-Subscription-Key",
        pattern=RegEx.sub_key,
    ),
    # pylint: enable=unused-argument
) -> TestLawOutput:
    """
    Get query status
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running azure_test_law function...")
    # extract fields
    workspace_id = validated_inputs.workspace_id
    kql_query = validated_inputs.kql_query
    # connection
    try:
        log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
        response = log_client.query_workspace(
            workspace_id=workspace_id,
            query=kql_query,
            timespan=None,
            server_timeout=600,
        )
        table = response.tables[0]
        df = pd.DataFrame(data=table.rows, columns=table.columns)
        logging.info("Success: %s", df)
        return_response = {"response": "success", "results": f"shape: {str(df.shape)}"}
        return return_response
    except Exception as e:
        logging.error("Failed: %s", e)
        raise HTTPException(status_code=500, detail=f"Failed: {e}") from e


# --------------------------------------------------------------------------------------
# Azure Functions - Queue Triggers
# --------------------------------------------------------------------------------------

# fix for message encoding errors (default is base64):
# add "extensions": {"queues": {"messageEncoding": "none"}} to host.json
# failed messages are sent to <QUEUE_NAME>-poison

bp = func.Blueprint()


@bp.queue_trigger(
    arg_name="msg",
    queue_name=env_var_queue_query_name,
    connection="storageAccountConnectionString",
)
def azure_queue_query(msg: func.QueueMessage) -> None:
    """
    Azure Function that triggers on messages in Query Queue
    Submits query for processing
    """
    logging.info("Python storage queue event triggered")
    logging.info("Running azure_queue_query function...")
    start_time = time.time()
    # process message
    message_content = msg.get_json()
    query_message_validation_check(message_content)
    # extract fields
    query_uuid = message_content["QueryUUID"]
    subscription_id = message_content["Subscription"]
    resource_group_name = message_content["ResourceGroup"]
    log_analytics_worksapce_name = message_content["LogAnalyticsWorkspace"]
    log_analytics_workspace_id = message_content["LogAnalyticsWorkspaceId"]
    storage_queue_url = message_content["StorageQueueURL"]
    storage_queue_name = message_content["StorageQueueName"]
    storage_blob_url = message_content["StorageBlobURL"]
    storage_blob_container_name = message_content["StorageContainer"]
    storage_table_url = message_content["StorageTableURL"]
    storage_table_query_name = message_content["StorageTableQueryName"]
    storage_table_process_name = message_content["StorageTableProcessName"]
    table_names_and_columns = message_content["TableNamesColumns"]
    start_datetime = message_content["StartDatetime"]
    end_datetime = message_content["EndDatetime"]
    query_row_limit = message_content["QueryRowLimit"]
    query_row_limit_correction = message_content["QueryRowLimitCorrection"]
    break_up_query_freq = message_content["BreakUpQueryFreq"]
    storage_blob_output_format = message_content["StorageBlobOutputFormat"]
    try:
        results = query_log_analytics_send_to_queue(
            query_uuid,
            credential,
            subscription_id,
            resource_group_name,
            log_analytics_worksapce_name,
            log_analytics_workspace_id,
            storage_queue_url,
            storage_queue_name,
            storage_blob_url,
            storage_blob_container_name,
            storage_table_url,
            storage_table_query_name,
            storage_table_process_name,
            table_names_and_columns,
            start_datetime,
            end_datetime,
            query_row_limit=query_row_limit,
            query_row_limit_correction=query_row_limit_correction,
            break_up_query_freq=break_up_query_freq,
            storage_blob_output_format=storage_blob_output_format,
        )
        time_calculation = round(time.time() - start_time, 1)
        logging.info("Success, Runtime: %s seconds", time_calculation)
        result_response = {
            "query_uuid": results["PartitionKey"],
            "query_submit_status": results["Status"],
            "table_names": results["Tables"],
            "start_datetime": results["StartDatetime"],
            "end_datetime": results["EndDatetime"],
            "total_row_count": results["TotalRowCount"],
            "subqueries_generated": results["MessagesGenerated"],
            "subqueries_sent_to_queue": results["MessagesSentToQueue"],
            "runtime_seconds": results["RuntimeSeconds"],
            "query_submit_datetime": results["TimeGenerated"],
        }
        logging.info("Results: %s", result_response)
    except Exception as e:
        logging.error("Failed to process queue message: %s %s", message_content, e)
        raise Exception(f"Failed to process queue message: {message_content}") from e


@bp.queue_trigger(
    arg_name="msg",
    queue_name=env_var_queue_process_name,
    connection="storageAccountConnectionString",
)
def azure_queue_process(msg: func.QueueMessage) -> None:
    """
    Azure Function that triggers on messages in Process Queue
    Processes query and exports to stroage account
    """
    logging.info("Python storage queue event triggered")
    logging.info("Running azure_queue_process function...")
    start_time = time.time()
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential, endpoint=env_var_law_endpoint)
    # process message: validate, query log analytics, and send results to storage
    message_content = msg.get_json()
    try:
        process_queue_message(
            log_client,
            message_content,
        )
        time_calculation = round(time.time() - start_time, 1)
        logging.info("Success, Runtime: %s seconds", time_calculation)
    except Exception as e:
        logging.error("Failed to process queue message: %s %s", message_content, e)
        raise Exception(f"Failed to process queue message: {message_content}") from e


@bp.queue_trigger(
    arg_name="msg",
    queue_name=poison_queue_query_name,
    connection="storageAccountConnectionString",
)
def azure_queue_query_poison(msg: func.QueueMessage) -> None:
    """
    Azure Function that triggers on poisoned messages in Query Queue
    Sends failure information to storage table log
    """
    logging.info("Python storage queue event triggered")
    logging.info("Running azure_queue_query_poison function...")
    start_time = time.time()
    try:
        # validate message
        message = msg.get_json()
        query_message_validation_check(message)
        logging.info("Processing Message: %s", message)
        # storage table connection for logging
        # note: requires Storage Table Data Contributor role
        storage_table_url = message["StorageTableURL"]
        storage_table_name = message["StorageTableQueryName"]
        table_client = TableClient(
            storage_table_url, storage_table_name, credential=credential
        )
        # extract fields
        query_uuid = message["QueryUUID"]
        table_names_and_columns = message["TableNamesColumns"]
        table_names_join = ", ".join(table_names_and_columns.keys())
        start_datetime = message["StartDatetime"]
        start_datetime = start_datetime.replace("T", " ").replace("Z", "")
        end_datetime = message["EndDatetime"]
        end_datetime = end_datetime.replace("T", " ").replace("Z", "")
        # logging to storage table
        time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
        status = "Failed"
        # generate unique row key
        row_key = f"{query_uuid}__{status}__{table_names_join}__"
        row_key += f"{start_datetime}__{end_datetime}"
        unique_row_sha256_hash = hashlib.sha256(row_key.encode()).hexdigest()
        return_message = {
            "PartitionKey": query_uuid,
            "RowKey": unique_row_sha256_hash,
            "Status": status,
            "Tables": table_names_join,
            "StartDatetime": start_datetime,
            "EndDatetime": end_datetime,
            "TimeGenerated": time_generated,
        }
        table_client.upsert_entity(return_message, mode=UpdateMode.REPLACE)
        runtime_calculation = round(time.time() - start_time, 1)
        logging.info("Success, Runtime: %s seconds", runtime_calculation)
    except Exception as e:
        message_body_decoded = msg.get_body().decode("utf-8")
        logging.error("Invalid message: %s", message_body_decoded)
        raise Exception("Failed, Invalid message") from e


@bp.queue_trigger(
    arg_name="msg",
    queue_name=poison_queue_process_name,
    connection="storageAccountConnectionString",
)
def azure_queue_process_poison(msg: func.QueueMessage) -> None:
    """
    Azure Function that triggers on poisoned messages in Process Queue
    Sends failure information to storage table log
    """
    logging.info("Python storage queue event triggered")
    logging.info("Running azure_queue_process_poison function...")
    start_time = time.time()
    try:
        # validate message
        message = msg.get_json()
        process_message_validation_check(message)
        logging.info("Processing Message: %s", message)
        # storage table connection for logging
        # note: requires Storage Table Data Contributor role
        storage_table_url = message["StorageTableURL"]
        storage_table_name = message["StorageTableName"]
        table_client = TableClient(
            storage_table_url, storage_table_name, credential=credential
        )
        # extract fields
        query_uuid = message["QueryUUID"]
        table_name = message["Table"]
        start_datetime = message["StartDatetime"]
        start_datetime = start_datetime.replace("T", " ").replace("Z", "")
        end_datetime = message["EndDatetime"]
        end_datetime = end_datetime.replace("T", " ").replace("Z", "")
        row_count = message["Count"]
        # logging to storage table
        time_generated = pd.Timestamp.today("UTC").strftime("%Y-%m-%d %H:%M:%S.%f")
        status = "Failed"
        # generate unique row key
        row_key = f"{query_uuid}__{status}__{table_name}__"
        row_key += f"{start_datetime}__{end_datetime}__{row_count}"
        unique_row_sha256_hash = hashlib.sha256(row_key.encode()).hexdigest()
        return_message = {
            "PartitionKey": query_uuid,
            "RowKey": unique_row_sha256_hash,
            "Status": status,
            "Table": table_name,
            "StartDatetime": start_datetime,
            "EndDatetime": end_datetime,
            "RowCount": row_count,
            "TimeGenerated": time_generated,
        }
        table_client.upsert_entity(return_message, mode=UpdateMode.REPLACE)
        runtime_calculation = round(time.time() - start_time, 1)
        logging.info("Success, Runtime: %s seconds", runtime_calculation)
    except Exception as e:
        message_body_decoded = msg.get_body().decode("utf-8")
        logging.error("Invalid message: %s", message_body_decoded)
        raise Exception("Failed, Invalid message") from e


app = func.AsgiFunctionApp(app=fastapi_app, http_auth_level=func.AuthLevel.FUNCTION)
app.register_functions(bp)
