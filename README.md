# Azure Log Analytics Data Export

## Summary

This Azure Function App enables the export of big data (10M+ records per hour) from Azure Log Analytics to Blob Storage via Python SDKs. In testing, 50M records with 10 columns were successfully exported in approximately 1 hour using a Consumption (Serverless) hosting plan.

This work expands upon: [How to use logic apps to handle large amounts of data from log analtyics](https://techcommunity.microsoft.com/t5/azure-integration-services-blog/how-to-use-logic-apps-to-handle-large-amount-of-data-from-log/ba-p/2797466)

<b>Inputs and Outputs</b>:
- <b>Input</b>: log analytics workspace table(s), columns, and date range
- <b>Output</b>: JSON (list, line delimited), CSV, or PARQUET files

<b>Azure Functions</b>:
1. <b>azure_ingest_test_data()</b>: HTTP Trigger, creates and ingests test data (optional)
2. <b>azure_submit_query()</b>: HTTP Trigger, divides request into smaller queries/jobs and sends to storage queue
3. <b>azure_process_queue()</b>: Storage Queue Trigger, runs jobs from the storage queue and saves results to storage account
4. <b>azure_process_poison_queue()</b>: Storage Queue Trigger, processes poison queue message and sends to table storage log
5. <b>azure_get_query_status()</b>: HTTP Trigger, gives high-level status of query (number of sub-queries, successes, failures, row counts, file sizes)

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/004b118e-a6ff-4557-aa3b-439dd962b26d)

## Files

- <b>azure-log-analytics-data-export.ipynb</b>: python notebook for development, testing, or interactive use
- <b>function_app.py</b>: Azure Function App python source code
- <b>host.json</b>: Azure Function App settings
- <b>requirements.txt</b>: python package requirements file

## Setup Notes

<b>Azure Resources</b>:
1. Log Analytics Workspace (data source)
2. Storage Account
- Container (data output destination)
- Queue (temp storage for split query messages/jobs)
- Table (logging for status checks)
3. Azure Function App (Python 3.11+, consumption or premium plan)
4. Azure API Management (frontend user interface, optional) 

<b>Authentication Method (Managed Identity or Service Principal) Requirements</b>:
- Setup via Azure Portal -> Function App -> Identity -> System Assigned -> On -> Azure Role Assignments
1. <b>Monitoring Metrics Publisher</b>: Ingest to Log Analytics (optional)
2. <b>Log Analytics Contributor</b>: Query Log Analytics
3. <b>Storage Queue Data Contributor</b>: Storage Queue Send/Get/Delete
4. <b>Storage Queue Data Message Processor</b>: Storage Queue Trigger for Azure Function
5. <b>Storage Blob Data Contributor</b>: Upload to Blob Storage
6. <b>Storage Table Data Contributor</b>: Logging

<b>Required Environment Variables for Queue Trigger via Managed Identity</b>: 
- Setup via Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
1. <b>QueueName</b> -> <QUEUE_NAME>
2. <b>storageAccountConnectionString__queueServiceUri</b> -> https://<STORAGE_ACCOUNT>.queue.core.windows.net/
3. <b>storageAccountConnectionString__credential</b> -> managedidentity

<b>Data Collection Endpoint and Rule Setup for Log Analytics Ingest</b>:
1. Azure Portal -> Monitor -> Create Data Collection Endpoint
2. Azure Portal -> Log Analytics -> Table -> Create New Custom Table
3. Reference: [Tutorial: Send data to Azure Monitor Logs with Logs ingestion API (Azure portal)
](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal)

<b>Azure Storage Setup</b>:
1. Create a container for data output files
2. Create a queue for messages/jobs
3. Create a queue named <QUEUE_NAME>-poison for failed messages/jobs
4. Create 3 tables for logging (i.e. ingestlog, querylog, and processlog)

<b>Queue Trigger Setup:</b>:
- To fix message encoding errors (default is base64), add "extensions": {"queues": {"messageEncoding": "none"}} to host.json
- Note: Failed messages/jobs are sent to <QUEUE_NAME>-poison
  
## Usage

1. (Optional) Execute HTTP trigger <b>azure_ingest_test_data()</b> to generate test/fake data and ingest into Log Analytics. 

```json
{
    "log_analytics_data_collection_endpoint" : "https://XXXXXXXXXXXXXX-XXXXXXX.XXXXXX.ingest.monitor.azure.com",
    "log_analytics_data_collection_rule_id" : "dcr-XXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "log_analytics_data_collection_stream_name" : "Custom-XXXXXXXXXXXXXXXX_CL",
    "storage_table_url" : "https://XXXXXXXXXXXXXXXXXXXX.table.core.windows.net/",
    "storage_table_ingest_name" : "XXXXXXXXXXX",
    "start_datetime" : "2024-03-19 00:00:00.000000",
    "timedelta_seconds" : 0.00036,
    "number_of_rows" : 1000000
}
```

2. Execute HTTP trigger <b>azure_submit_query()</b> with query and connection parameters:

```json
{
    "subscription_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "resource_group_name" : "XXXXXXXXXXXXXXXXXXXXXXX",
    "log_analytics_worksapce_name" : "XXXXXXXXXXXXXXXX",
    "log_analytics_workspace_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "storage_queue_url" : "https://XXXXXXXXXXXXXXXXXXX.queue.core.windows.net/",
    "storage_queue_name" : "XXXXXXXXXXXXXX",
    "storage_blob_url" : "https://XXXXXXXXXXXXXXXXXXXXX.blob.core.windows.net/",
    "storage_blob_container_name" : "XXXXXXXXXXXXX",
    "storage_table_url" : "https://XXXXXXXXXXXXXXXXXXXXXX.table.core.windows.net/",
    "storage_table_query_name" : "XXXXXXXXXXXXXX",
    "storage_table_process_name" : "XXXXXXXXXXXXXX",
    "table_names_and_columns" : { "XXXXXXXXXXXXXXX_CL": ["TimeGenerated","DataColumn1","DataColumn2","DataColumn3","DataColumn4","DataColumn5","DataColumn6","DataColumn7","DataColumn8","DataColumn9"]},
    "start_datetime" : "2024-03-19 00:00:00",
    "end_datetime" : "2024-03-20 00:00:00"
}
```
The query will be split into chunks and then saved as messages in a storage queue. Next, log_analytics_process_queue(), which is queue triggered, will automatically processes the messages in parallel and send the results to a storage account container. 

3. Execute HTTP trigger <b>azure_get_query_status()</b> with query uuid and connection parameters:

```json
{
    "query_uuid" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "storage_table_url" : "https://XXXXXXXXXXXXXXXXXX.table.core.windows.net/",
    "storage_table_query_name" : "XXXXXXXXX",
    "storage_table_process_name" : "XXXXXXXXXXXXX"
}
```

## Changelog

1.3.0:
- Added pydantic input validation

1.2.0:
- Added get_status() azure function

1.1.0:
- Added logging to Azure Table Storage
- Added row count checks

1.0.0:
- Initial release
