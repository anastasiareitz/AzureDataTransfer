# Azure Log Analytics Data Export

## Summary

This Azure Function App enables the export of big data (10M+ records per hour) from Azure Log Analytics to Blob Storage via Python SDKs. In testing, 50M records with 10 columns were successfully exported in approximately 1 hour using a Consumption (Serverless) hosting plan.

This work expands upon: [How to use logic apps to handle large amounts of data from log analtyics](https://techcommunity.microsoft.com/t5/azure-integration-services-blog/how-to-use-logic-apps-to-handle-large-amount-of-data-from-log/ba-p/2797466)

<b>Inputs and Outputs</b>:
- <b>Input</b>: table(s), columns, and date range
- <b>Output</b>: JSON (list, line delimited), CSV, or PARQUET files

<b>Azure Functions</b>:
1. <b>log_analytics_generate_test_data()</b>: HTTP Trigger, creates and ingests test data (optional)
2. <b>log_analytics_query_send_to_queue()</b>: HTTP Trigger, divides request into smaller queries/jobs and sends to storage queue
3. <b>log_analytics_process_queue()</b>: Storage Queue Trigger, runs jobs from the storage queue and saves results to storage account

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/648c59ff-50aa-4314-acc5-cb7a0539085a)

## Files

- <b>azure-log-analytics-data-export.ipynb</b>: python notebook for development, testing, or interactive use
- <b>function_app.py</b>: Azure Function App python source code
- <b>host.json</b>: Azure Function App settings
- <b>requirements.txt</b>: python package requirements file

## Setup Notes

<b>Azure Resources Required</b>:
1. Log Analytics Workspace (your data source)
2. Storage Account
- Queue (temp storage for split query messages/jobs)
- Container (your data destination)
3. Azure Function App (Python 3.11+, consumption or premium plan)

<b>Authentication Method (Managed Identity or Service Principal) Requirements</b>:
- Setup via Azure Portal -> Function App -> Identity -> System Assigned -> On -> Azure Role Assignments
1. <b>Monitoring Metrics Publisher</b>: Ingest to Log Analytics (optional)
2. <b>Log Analytics Contributor</b>: Query Log Analytics
3. <b>Storage Queue Data Contributor</b>: Storage Queue Send/Get/Delete
4. <b>Storage Queue Data Message Processor</b>: Storage Queue Trigger for Azure Function
5. <b>Storage Blob Data Contributor</b>: Upload to Blob Storage

<b>Required Environment Variables</b>:
- Setup via Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
1. <b>QueueName</b> -> <QUEUE_NAME>
2. <b>StorageBlobURL</b> -> https://<STORAGE_ACCOUNT_NAME>.blob.core.windows.net/
3. <b>StorageBlobContainer</b> -> <STORAGE_ACCOUNT_CONTAINER_NAME>
4. <b>StorageOutputFormat</b> -> <OUTPUT_FORMAT> (JSONL, CSV, or PARQUET)

<b>Required Environment Variables for Queue Trigger via Managed Identity</b>: 
1. <b>storageAccountConnectionString__queueServiceUri</b> -> https://<STORAGE_ACCOUNT>.queue.core.windows.net/
2. <b>storageAccountConnectionString__credential</b> -> managedidentity

<b>Data Collection Endpoint and Rule Setup for Log Analytics Ingest</b>:
1. Azure Portal -> Monitor -> Create Data Collection Endpoint
2. Azure Portal -> Log Analytics -> Table -> Create New Custom Table
3. Create Data Collection Rule and Add Publisher Role 
reference: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal

## Usage

(Optional) Execute HTTP trigger log_analytics_generate_test_data() to generate test/fake data and ingest into Log Analytics. 

```json
{
    "log_analytics_data_collection_endpoint" : "https://XXXXXXXXX-XXXXX.eastus-1.ingest.monitor.azure.com",
    "log_analytics_data_collection_rule_id" : "dcr-XXXXXXXXXXXXXXXXXXXXXX",
    "log_analytics_data_collection_stream_name" : "Custom-XXXXXXXXXXXX_CL",
    "start_datetime" : "03-06-2024 00:00:00.000000",
    "timedelta_seconds" : 0.00036,
    "number_of_rows" : 1000000
}
```

Execute HTTP trigger log_analytics_query_send_to_queue() with query and connection parameters:

```json
{
    "subscription_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "resource_group_name" : "XXXXXXXXXXXXXXXXXXX",
    "log_analytics_worksapce_name" : "XXXXXXXXXXX",
    "log_analytics_workspace_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "storage_queue_url" : "https://XXXXXXXXXXXXXXX.queue.core.windows.net/XXXXXXXXXXXXX",
    "table_names_and_columns" : { "XXXXXXXXXXXX_CL": ["TimeGenerated","DataColumn1","DataColumn2","DataColumn3","DataColumn4","DataColumn5","DataColumn6","DataColumn7","DataColumn8","DataColumn9"]},
    "start_datetime" : "2024-03-06 00:00:00",
    "end_datetime" : "2024-03-06 01:00:00"
}
```
The query will be split into chunks and then saved as messages in a storage queue. Next, log_analytics_process_queue(), which is queue triggered, will automatically processes the messages in parallel and send the results to a storage account container. 

## Changelog

1.0.0:
- Initial Release 
