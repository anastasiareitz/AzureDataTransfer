import json
import logging
import math
import os
import random
import string
import time
from io import BytesIO, StringIO

import pandas as pd
import pyarrow
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.monitor.query import LogsQueryClient, LogsQueryStatus
from azure.storage.blob import ContainerClient
from azure.storage.queue import QueueClient, QueueMessage

# azure function app
app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

# azure auth via managed identity
# Azure Portal -> Function App -> Identity -> System Assigned
# Note: requires the following roles:
# 1. Monitoring Metrics Publisher
# 2. Log Analytics Contributor
# 3. Storage Queue Data Contributor
# 4. Storage Queue Data Message Processor
# 5. Storage Blob Data Contributor
credential = DefaultAzureCredential()

# setup for storage queue trigger via managed identity
# environment variables
# Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
# add 1. storageAccountConnectionString__queueServiceUri -> https://<STORAGE_ACCOUNT>.queue.core.windows.net/
# add 2. storageAccountConnectionString__credential -> managedidentity
# add 3. QueueName -> <QUEUE_NAME>
# add 4. Repeat for StorageBlobURL, StorageBlobContainer, StorageOutputFormat
env_var_storage_queue_name = os.environ["QueueName"]
env_var_storage_blob_url = os.environ["StorageBlobURL"]
env_var_storage_blob_container = os.environ["StorageBlobContainer"]
env_var_output_format = os.environ["StorageOutputFormat"]


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
    for each_index in range(1, number_of_columns):
        each_column_name = f"DataColumn{each_index}"
        each_column_value = "".join(
            random.choice(string.ascii_lowercase) for i in range(random_length)
        )
        fake_data_df[each_column_name] = each_column_value
    # convert datetime to string column to avoid issues in log analytics
    time_generated = fake_data_df["TimeGenerated"].dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    fake_data_df["TimeGenerated"] = time_generated
    # status
    logging.info(f"Data Shape: {fake_data_df.shape}")
    logging.info(f"Size: {fake_data_df.memory_usage().sum() / 1_000_000} MBs")
    logging.info(f"First Datetime: {fake_data_df['TimeGenerated'].iloc[0]}")
    logging.info(f"Last Datetime: {fake_data_df['TimeGenerated'].iloc[-1]}")
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
        logging.info(f"Error sending to log analytics, will skip: {e}")
        return 0


def generate_and_ingest_test_data(
    credential: DefaultAzureCredential,
    endpoint: str,
    rule_id: str,
    stream_name: str,
    start_date: str,
    timedelta_seconds: float,
    number_of_rows: int,
    number_of_columns: int = 10,
    max_rows_per_request=5_000_000,
) -> pd.DataFrame:
    """
    Generates fake data and ingests in Log Analytics for testing
        note: credential requires Log Analytics Contributor and Monitor Publisher roles
        note: 10M rows with 10 columns takes about 15-20 minutes
    Log Analytics Data Collection Endpoint and Rule setup:
        1. azure portal -> monitor -> create data collection endpoint
        2. azure portal -> log analytics -> table -> create new custom table in log analytics
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
        start_date: date to insert fake data
            format: "02-08-2024 00:00:00.000000"
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
        dict with results stats
    """
    time_start = time.time()
    # input validation
    given_timestamp = pd.to_datetime(start_date)
    current_datetime = pd.to_datetime("today")
    check_start_range = current_datetime - pd.to_timedelta("2D")
    check_end_range = current_datetime + pd.to_timedelta("1D")
    if not (check_start_range <= given_timestamp <= check_end_range):
        logging.info("Warning: Date given is outside allowed ingestion range")
        logging.info("Note: Log Analytics will use ingest time as TimeGenerated")
    if number_of_rows < 2 or number_of_columns < 2:
        raise Exception("invalid row and/or column numbers")
    # log analytics ingest connection
    ingest_client = LogsIngestionClient(endpoint, credential)
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
        logging.info(
            f"Generating Test Data Request {each_index} of {number_of_ingests}..."
        )
        try:
            each_fake_data_df = generate_test_data(
                each_start_datetime,
                timedelta_seconds,
                each_number_of_rows,
                number_of_columns,
            )
        except Exception as e:
            logging.info(f"Unable to generate test data: {e}")
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
        logging.info(
            f"Runtime: {round(time.time() - each_request_start_time, 1)} seconds"
        )
    # return results
    return {
        "rows_sent": successfull_rows_sent,
        "rows_total": number_of_rows,
        "runtime_seconds": round(time.time() - time_start, 1),
    }


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
        raise Exception(f"Failed Log Analytics Request, Exception: {e}")
    finally:
        time.sleep(request_wait_seconds)


def query_log_analytics_connection_request(
    credential: DefaultAzureCredential, workspace_id: str, kql_query: str
) -> pd.DataFrame:
    # log analytics connection
    # note: need to add Log Analytics Contributor and Monitor Publisher role
    log_client = LogsQueryClient(credential)
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
        # if no column names provided, query log analytics for all column names
        else:
            logging.info(f"Getting columns names for {each_table}")
            each_kql_query = f"""
            let TABLE_NAME = "{each_table}";
            table(TABLE_NAME)
            | project-away TenantId, Type, _ResourceId
            | take 1
            """
            each_df = query_log_analytics_request(
                workspace_id, log_client, each_kql_query
            )
            each_columns_fix = list(each_df.columns)
            each_columns_fix.remove("TimeGenerated")
            each_columns_fix = ["TimeGenerated"] + each_columns_fix
            logging.info(f"Columns Detected: {each_columns_fix}")
            output[each_table] = each_columns_fix
    if len(output) == 0:
        raise Exception("No valid table names")
    return output


def break_up_initial_date_range(
    table_name: str, start_datetime: str, end_datetime: str, freq: str
) -> pd.DataFrame:
    # break up date range
    date_range = pd.date_range(start=start_datetime, end=end_datetime, freq=freq)
    date_range = [str(each) for each in date_range.to_list()]
    # fix for final timestamp
    date_range += [end_datetime]
    if date_range[-1] == date_range[-2]:
        date_range.pop(-1)
    time_pairs = [
        (date_range[i], date_range[i + 1]) for i in range(len(date_range) - 1)
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
    logging.info(f"Splitting {table_name}: {start_datetime}-{end_datetime}")
    # query log analytics and get time ranges
    df = query_log_analytics_request(workspace_id, log_client, kql_query)
    # no results
    if df.shape[0] == 0:
        return pd.DataFrame()
    # datetime fix for events on final datetime
    # using copy and .loc to prevent pandas chaining warning
    df_copy = df.copy()
    final_endtime = df_copy["EndTime"].tail(1).item()
    new_final_endtime = str(
        pd.to_datetime(final_endtime) + pd.to_timedelta("0.0000001s")
    )
    new_final_endtime_fix_format = new_final_endtime.replace(" ", "T").replace(
        "00+00:00", "Z"
    )
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
    add_row_counts: bool,
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
    if add_row_counts:
        results_df = query_log_analytics_add_table_row_counts(
            results_df, workspace_id, log_client, table_name
        )
        # warning if query limit exceeded, change limits and try again
        if results_df.Count.gt(query_row_limit).any():
            raise Exception(
                f"Sub-Query exceeds query row limit, {list(results_df.Count)}"
            )
    # add table name column
    results_df.insert(loc=0, column="Table", value=[table_name] * len(results_df))
    return results_df


def query_log_analytics_split_query_rows_loop(
    df_queries: pd.DataFrame,
    workspace_id: str,
    log_client: LogsQueryClient,
    query_row_limit: int,
    query_row_limit_correction: int,
    add_row_counts: bool,
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
            add_row_counts,
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
    table_names_and_columns: dict,
    subscription_id: str,
    resource_group: str,
    worksapce_name: str,
    workspace_id: str,
) -> list[dict]:
    # add column names
    column_names = query_results_df["Table"].apply(lambda x: table_names_and_columns[x])
    query_results_df.insert(loc=1, column="Columns", value=column_names)
    # add azure property columns
    query_results_df.insert(loc=2, column="Subscription", value=subscription_id)
    query_results_df.insert(loc=3, column="ResourceGroup", value=resource_group)
    query_results_df.insert(loc=4, column="LogAnalyticsWorkspace", value=worksapce_name)
    query_results_df.insert(loc=5, column="LogAnalyticsWorkspaceId", value=workspace_id)
    # convert to dictionary
    results = query_results_df.to_dict(orient="records")
    return results


def query_log_analytics_send_to_queue(
    credential: DefaultAzureCredential,
    subscription_id: str,
    resource_group: str,
    worksapce_name: str,
    workspace_id: str,
    storage_queue_url: str,
    table_names_and_columns: dict,
    start_datetime: str,
    end_datetime: str,
    query_row_limit: int = 250_000,
    query_row_limit_correction: int = 100,
    add_row_counts: bool = True,
    break_up_query_freq="4h",
) -> dict:
    """
    Generates
        note: credential requires Log Analytics Contributor and Storage Queue Data Contributor roles
        note: date range is processed as [start_datetime, end_datetime)
    Args:
        credential: azure default credential object
        subscription_id: azure subscription id
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        resource_group: azure resource group
        workspace_name: name of log analytics workspace
        workspace_id: log analytics workspace id
            format: "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
        storage_queue_url: storage account queue url
            format: "https://{storage_account_name}.queue.core.windows.net/{queue_name}"
        table_names: dictionary of table names with columns to project
            note: blank column list will detect and use all columns
            format:  {"table_name" : ["column_1", "column_2", ... ], ... }
        start_datetime: starting datetime, inclusive
            format: YYYY-MM-DD HH:MM:SS
        end_datetime: ending datetime, exclusive
            format: YYYY-MM-DD HH:MM:SS
        query_row_limit: max number of rows for each follow-up query/message
        query_row_limit_correction: correction factor in case of overlapping data
        add_row_counts: adds expected row count for queries to messages
        break_up_query_freq: limit on query datetime range to prevent crashes
            note:for  more than 10M rows / hour, use 4 hours or less
    Return
        dict of results stats
    """
    start_time = time.time()
    # input validation
    try:
        pd.to_datetime(start_datetime)
        pd.to_datetime(end_datetime)
    except Exception as e:
        raise Exception(f"Invalid Datetime Format, Exception {e}")
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential)
    # storage queue connection
    # note: need to add Storage Queue Data Contributor role
    queue_client = QueueClient.from_queue_url(storage_queue_url, credential)
    # process table and column names
    table_names_and_columns = query_log_analytics_get_table_columns(
        table_names_and_columns, workspace_id, log_client
    )
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
        add_row_counts,
    )
    if not query_results_df.empty:
        # process results, add columns, and convert to list of dicts
        results = process_query_results_df(
            query_results_df,
            table_names_and_columns,
            subscription_id,
            resource_group,
            worksapce_name,
            workspace_id,
        )
        number_of_results = len(results)
        # send to queue
        successful_sends = 0
        logging.info(f"Initial Queue Status: {queue_client.get_queue_properties()}")
        for each_msg in results:
            each_result = send_message_to_queue(queue_client, each_msg)
            if each_result == "Success":
                successful_sends += 1
        logging.info(f"Messages Successfully Sent to Queue: {successful_sends}")
        logging.info(f"Updated Queue Status: {queue_client.get_queue_properties()}")
        # return results
        return {
            "messages_generated": number_of_results,
            "messages_sent": successful_sends,
            "runtime_seconds": round(time.time() - start_time, 1),
        }
    # no results
    else:
        logging.info("Error: No Query Messages Generated")
        return {
            "messages_generated": 0,
            "messages_sent": 0,
            "runtime_seconds": round(time.time() - start_time, 1),
        }


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
        logging.info(
            f"Error: Unable to send message to queue, skipped: {message}, exception: {e}"
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
        logging.info(f"Request Error: Unable to Get Queue Message, {e}")
        raise Exception(f"Request Error: Unable to Get Queue Message, {e}")
    finally:
        time.sleep(request_wait_seconds)


def delete_message_from_queue(queue_client: QueueClient, queue_message: QueueMessage):
    try:
        queue_client.delete_message(queue_message)
        logging.info(f"Successfully Deleted Message from Queue")
    except Exception as e:
        logging.info(f"Unable to delete message, {queue_message}, {e}")
        raise Exception(f"Unable to delete message, {queue_message}, {e}")


def check_if_queue_empty_peek_message(queue_client: QueueClient) -> bool:
    try:
        peek_messages = queue_client.peek_messages()
        if not peek_messages:
            return True
        return False
    except Exception as e:
        logging.info(f"Unable to peek at queue messages, {e}")
        return False


def message_validation_check(message: dict, confirm_row_count: bool):
    required_fields = [
        "Table",
        "Columns",
        "Subscription",
        "ResourceGroup",
        "LogAnalyticsWorkspace",
        "LogAnalyticsWorkspaceId",
        "StartTime",
        "EndTime",
    ]
    if confirm_row_count:
        required_fields += ["Count"]
    if not all(each_field in message for each_field in required_fields):
        logging.info(f"Invalid message, required fields missing: {message}")
        raise Exception(f"Invalid message, required fields missing: {message}")


def query_log_analytics_get_query_results(
    log_client: LogsQueryClient, message: dict
) -> pd.DataFrame:
    # extract message fields
    workspace_id = message["LogAnalyticsWorkspaceId"]
    table_name = message["Table"]
    column_names = message["Columns"]
    start_datetime = message["StartTime"]
    end_datetime = message["EndTime"]
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


def datetime_to_filename_safe(input: str) -> str:
    # remove characters from timestamp to be filename safe/readable
    output = input.replace("-", "").replace(":", "").replace(".", "")
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
    start_datetime = message["StartTime"]
    end_datetime = message["EndTime"]
    # datetime conversion via pandas: dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    # extract datetime values for filename
    extract_year = output_filename_timestamp.strftime("%Y")
    extract_month = output_filename_timestamp.strftime("%m")
    extract_day = output_filename_timestamp.strftime("%d")
    extract_hour = output_filename_timestamp.strftime("%H")
    # mimics continuous export from log analytics
    # https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-data-export
    output_filename = f"{table_name}/"
    output_filename += f"WorkspaceResourceId=/"
    output_filename += f"subscriptions/{subscription}/"
    output_filename += f"resourcegroups/{resource_group}/"
    output_filename += f"providers/microsoft.operationalinsights/"
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
    elif output_format == "CSV":
        output_filename += ".csv"
        output_data = results_df.to_csv(index=False)
    elif output_format == "PARQUET":
        output_filename += ".parquet"
        output_data = results_df.to_parquet(index=False, engine="pyarrow")
    return output_filename, output_data


def process_queue_message(
    log_client: LogsQueryClient,
    container_client: ContainerClient,
    message: dict,
    output_format: str,
    confirm_row_count: bool,
):
    """
    Processes 1 message: validates, queries log analytics, and saves results to storage account
    Args:
        log_client: azure log analytics LogsQueryClient object
        container_client: azure storage account ContainerClient object
        message: message content dictionary
        output_format: output file format, options = "JSONL", "CSV", "PARQUET
            note: JSONL is json line delimited
        confirm_row_count: enables check if row count in message matches downloaded data
    Returns:
        None
    """
    logging.info(f"Processing Message: {message}")
    # validate message
    message_validation_check(message, confirm_row_count)
    # query log analytics
    query_results_df = query_log_analytics_get_query_results(log_client, message)
    logging.info(
        f"Successfully Downloaded from Log Analytics: {query_results_df.shape}"
    )
    if confirm_row_count:
        if query_results_df.shape[0] != message["Count"]:
            logging.info(f"Row count doesn't match expected value, {message}")
            raise Exception(f"Row count doesn't match expected value, {message}")
    # output filename and file format
    output_filename_timestamp = query_results_df["TimeGenerated"].iloc[0]
    output_filename_base = generate_output_filename_base(
        message, output_filename_timestamp
    )
    full_output_filename, output_data = output_filename_and_format(
        query_results_df, output_format, output_filename_base
    )
    # upload to storage
    upload_file_to_storage(container_client, full_output_filename, output_data)


def process_queue_messages_loop(
    credential: DefaultAzureCredential,
    storage_queue_url: str,
    storage_blob_url_and_container: list[str],
    output_format: str = "JSONL",
    confirm_row_count: bool = True,
    message_visibility_timeout_seconds: int = 600,
) -> dict:
    """
    Processes Log Analytics query jobs/messages from a storage queue and exports to Blob Storage
        note: credential requires Log Analytics Contributor, Storage Queue Data Contributor, and Storage Blob Data Contributor roles
        note: takes ~150 seconds for a query with 500k rows and 10 columns to csv (100 seconds for parquet)
        note: intended to be run interactively, for example, in a notebook or terminal
    Args:
        credential: azure default credential object
        storage_queue_url: storage account queue url
            format: "https://{storage_account_name}.queue.core.windows.net/{queue_name}"
        storage_blob_url_and_container: storage account blob url and container name
            format: ["https://{storage_account_name}.blob.core.windows.net/", "{container_name}"]
        output_format: output file format, options = "JSONL", "CSV", "PARQUET
            note: JSONL is json line delimited
        confirm_row_count: enables check if row count in message matches downloaded data
        message_visibility_timeout_seconds: number of seconds for queue message visibility
    Returns:
        dict of results stats
    """
    # input validation
    logging.info(
        f"Processing Queue Messages, press CTRL+C or interupt kernel button to stop..."
    )
    start_time = time.time()
    support_file_formats = ["JSONL", "CSV", "PARQUET"]
    if output_format not in support_file_formats:
        raise Exception("File format not supported")
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential)
    # storage queue connection
    # note: need to add Storage Queue Data Contributor role
    queue_client = QueueClient.from_queue_url(storage_queue_url, credential)
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    storage_blob_url, storage_container_name = storage_blob_url_and_container
    container_client = ContainerClient(
        storage_blob_url, storage_container_name, credential
    )
    # process messages from queue until empty
    successful_messages = 0
    failed_messages = 0
    try:
        # loop through all messages in queue
        while True:
            each_start = time.time()
            # queue status
            logging.info(f"Queue Status: {queue_client.get_queue_properties()}")
            # get message
            queue_message = get_message_from_queue(
                queue_client, message_visibility_timeout_seconds
            )
            if queue_message:
                try:
                    # extract content
                    message_content = json.loads(queue_message.content)
                    # process message: validate, query log analytics, upload to storage
                    process_queue_message(
                        log_client,
                        container_client,
                        message_content,
                        output_format,
                        confirm_row_count,
                    )
                    # remove message from queue if successful
                    delete_message_from_queue(queue_client, queue_message)
                    logging.info(
                        f"Runtime: {round(time.time() - each_start, 1)} seconds"
                    )
                    successful_messages += 1
                except Exception as e:
                    logging.info(
                        f"Unable to process message: {queue_message.content} {e}"
                    )
                    failed_messages += 1
                    continue
            # queue empty
            else:
                logging.info(
                    f"Waiting for message visibility timeout ({message_visibility_timeout_seconds} seconds)..."
                )
                time.sleep(message_visibility_timeout_seconds + 60)
                # check if queue still empty
                if check_if_queue_empty_peek_message(queue_client):
                    logging.info(f"No messages in queue")
                    break
    # stop processing by keyboard interrupt
    except KeyboardInterrupt:
        logging.info(f"Run was cancelled manually by user")
    # return results
    finally:
        logging.info(f"Queue Status: {queue_client.get_queue_properties()}")
        logging.info(f"Processing queue messages complete")
        return {
            "successful_messages": successful_messages,
            "failed_messages": failed_messages,
            "runtime_seconds": round(time.time() - start_time, 1),
        }


# -----------------------------------------------------------------------------
# storage blob
# -----------------------------------------------------------------------------


def upload_file_to_storage(
    container_client: ContainerClient,
    filename: str,
    data: bytes | str,
    azure_storage_connection_timeout_fix_seconds: int = 600,
):
    # note: need to use undocumented param connection_timeout to avoid timeout errors
    # ref: https://stackoverflow.com/questions/65092741/solve-timeout-errors-on-file-uploads-with-new-azure-storage-blob-package
    try:
        blob_client = container_client.get_blob_client(filename)
        blob_client_output = blob_client.upload_blob(
            data=data,
            connection_timeout=azure_storage_connection_timeout_fix_seconds,
            overwrite=True,
        )
        storage_account_name = container_client.account_name
        container_name = container_client.container_name
        logging.info(
            f"Successfully Uploaded to Storage {storage_account_name}:{container_name}/{filename}"
        )
    except Exception as e:
        logging.info(f"Unable to upload, {filename}, {e}")
        raise Exception(f"Unable to upload, {filename}, {e}")


def download_blob(
    filename: str,
    credential: DefaultAzureCredential,
    storage_blob_url_and_container: list[str],
) -> pd.DataFrame:
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    storage_blob_url, storage_container_name = storage_blob_url_and_container
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
    credential: DefaultAzureCredential, storage_blob_url_and_container: list[str]
) -> pd.DataFrame:
    # increase column display for longer filenames
    pd.set_option("max_colwidth", None)
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    storage_blob_url, storage_container_name = storage_blob_url_and_container
    container_client = ContainerClient(
        storage_blob_url, storage_container_name, credential
    )
    # get blobs
    results = []
    for each_file in container_client.list_blobs():
        each_name = each_file.name
        each_size_MB = each_file.size / 1_000_000
        each_date = each_file.creation_time
        results.append([each_name, each_size_MB, each_date])
    # convert to dataframe
    df = pd.DataFrame(results, columns=["filename", "file_size_mb", "creation_time"])
    df = df.sort_values("creation_time", ascending=False)
    return df


# --------------------------------------------------------------------------------------
# Azure Functions
# --------------------------------------------------------------------------------------


@app.route(route="log_analytics_generate_test_data")
def log_analytics_generate_test_data(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function to generate test data and ingest to log analytics
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running log_analytics_generate_test_data function...")
    # request inputs
    request_body = req.get_json()
    log_analytics_data_collection_endpoint = request_body.get(
        "log_analytics_data_collection_endpoint"
    )
    log_analytics_data_collection_rule_id = request_body.get(
        "log_analytics_data_collection_rule_id"
    )
    log_analytics_data_collection_stream_name = request_body.get(
        "log_analytics_data_collection_stream_name"
    )
    start_datetime = request_body.get("start_datetime")
    timedelta_seconds = request_body.get("timedelta_seconds")
    number_of_rows = request_body.get("number_of_rows")
    # input validation
    if (
        log_analytics_data_collection_endpoint
        and log_analytics_data_collection_rule_id
        and log_analytics_data_collection_stream_name
        and start_datetime
        and timedelta_seconds
        and number_of_rows
    ):
        logging.info("Valid Inputs")
    else:
        return func.HttpResponse("Invalid Inputs", status_code=400)
    # generate fake data and ingest
    try:
        results = generate_and_ingest_test_data(
            credential,
            log_analytics_data_collection_endpoint,
            log_analytics_data_collection_rule_id,
            log_analytics_data_collection_stream_name,
            start_datetime,
            timedelta_seconds,
            number_of_rows,
        )
        logging.info(f"Success: {results}")
    except Exception as e:
        return func.HttpResponse(f"Failed: {e}", status_code=500)
    # response
    return func.HttpResponse(
        json.dumps(results), mimetype="application/json", status_code=200
    )


@app.route(route="log_analytics_query_send_to_queue")
def log_analytics_query_send_to_queue(
    req: func.HttpRequest,
) -> func.HttpResponse:
    """
    Azure Function to split query and send messages/jobs to storage queue
    """
    logging.info("Python HTTP trigger function processed a request")
    logging.info("Running log_analytics_query_send_to_queue function...")
    # request inputs
    request_body = req.get_json()
    subscription_id = request_body.get("subscription_id")
    resource_group_name = request_body.get("resource_group_name")
    log_analytics_worksapce_name = request_body.get("log_analytics_worksapce_name")
    log_analytics_workspace_id = request_body.get("log_analytics_workspace_id")
    storage_queue_url = request_body.get("storage_queue_url")
    table_names_and_columns = request_body.get("table_names_and_columns")
    start_datetime = request_body.get("start_datetime")
    end_datetime = request_body.get("end_datetime")
    # input validation
    if (
        subscription_id
        and resource_group_name
        and log_analytics_worksapce_name
        and log_analytics_workspace_id
        and storage_queue_url
        and table_names_and_columns
        and start_datetime
        and end_datetime
    ):
        logging.info("Valid Inputs")
    else:
        return func.HttpResponse("Invalid Inputs", status_code=400)
    # split query, generate messages, and send to queue
    try:
        results = query_log_analytics_send_to_queue(
            credential,
            subscription_id,
            resource_group_name,
            log_analytics_worksapce_name,
            log_analytics_workspace_id,
            storage_queue_url,
            table_names_and_columns,
            start_datetime,
            end_datetime,
        )
        logging.info(f"Success: {results}")
    except Exception as e:
        return func.HttpResponse(f"Failed: {e}", status_code=500)
    # response
    return func.HttpResponse(
        json.dumps(results), mimetype="application/json", status_code=200
    )


# note: to fix message encoding errors (default is base64):
# add "extensions": {"queues": {"messageEncoding": "none"}} to host.json
# failed messages are sent to <QUEUE_NAME>-poison
@app.queue_trigger(
    arg_name="msg",
    queue_name=env_var_storage_queue_name,
    connection="storageAccountConnectionString",
)
def log_analytics_process_queue(msg: func.QueueMessage) -> None:
    """
    Azure Function to processes messages/jobs in queue and send results to stoarge blob
    """
    logging.info(f"Python storage queue event triggered")
    logging.info("Running log_analytics_process_queue function...")
    start_time = time.time()
    # output format
    support_file_formats = ["JSONL", "CSV", "PARQUET"]
    if env_var_output_format not in support_file_formats:
        raise Exception("File format not supported")
    # log analytics connection
    # note: need to add Log Analytics Contributor role
    log_client = LogsQueryClient(credential)
    # storage blob connection
    # note: need to add Storage Blob Data Contributor role
    container_client = ContainerClient(
        env_var_storage_blob_url, env_var_storage_blob_container, credential
    )
    # process message: validate, query log analytics, and send results to storage
    message_content = msg.get_json()
    try:
        process_queue_message(
            log_client,
            container_client,
            message_content,
            env_var_output_format,
            confirm_row_count=True,
        )
        logging.info(f"Success, Runtime: {round(time.time() - start_time, 1)} seconds")
    except Exception as e:
        raise Exception(f"Failed: {e}")
