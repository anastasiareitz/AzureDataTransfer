# Azure Log Analytics Data Export

[Summary](#Summary) | [Files](#Files) | [Setup](#Setup) | [Usage](#Usage) | [Issues](#Issues) | [References](#References)

## Summary

This Azure Function App enables the export of big data (10M+ records per hour) from Azure Log Analytics to Blob Storage via Python SDKs, FastAPI, and API Management. In testing, 50M records with 10 columns were successfully exported in approximately 1 hour using a Consumption hosting plan.

<b>Inputs and Outputs</b>:
- <b>Input</b>: log analytics workspace table(s), columns, and date range
- <b>Output</b>: JSON (line delimited), CSV, or PARQUET files

<b>Azure FastAPI HTTP Functions</b>:
1. <b>azure_ingest_test_data()</b>: creates and ingests test data (optional)
2. <b>azure_submit_query()</b>: submits single query that is split into smaller queries/jobs and sends to queue
3. <b>azure_submit_query_parallel()</b>: breaks up initial query and submits multiple queries in parallel
4. <b>azure_get_status()</b>: gives high-level status of query (number of sub-queries, successes, failures, row counts, file sizes)

<b>Azure Queue Functions</b>:
1. <b>azure_queue_query()</b>: processes split queries
2. <b>azure_queue_process()</b>: processes subqueries and saves output to storage blobs 
3. <b>azure_queue_query_poison()</b>: processes invalid messages in query queue and saves to table log
4. <b>azure_queue_process_poison()</b>: processes invalid message in process queue and saves to table log

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/3bf68e1a-f300-4501-a5ce-94f142596876)

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/3c7651fa-cb89-460c-8632-6bf94a60b177)

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/bd175a25-de96-484d-a9cc-81dc9e6024a3)

## Files

- <b>azure-log-analytics-data-export.ipynb</b>: python notebook for development, testing, or interactive use
- <b>function_app.py</b>: Azure Function App python source code
- <b>host.json</b>: Azure Function App settings
- <b>requirements.txt</b>: python package requirements file
- <b>function.app</b>: zip push deployment 
- <b>zip-push-deployment.txt</b>: instructions on deploying streamlined function without APIM via CLI

## Setup

<b>You will need to have access to or provision the following Azure Resources</b>:
1. Log Analytics Workspace (data source)
2. Storage Account
- 1 Container (data output destination)
- 4 Queues (temp storage for split query messages/jobs)
- 3 Tables (logging)
3. Azure Function App (Python 3.11+, premium)
- Clone this repo, use VS Code, install Azure Functions tools/extension, deploy to Azure subscription
- Reference: [Create a function in Azure with Python using Visual Studio Code
](https://learn.microsoft.com/en-us/azure/azure-functions/create-first-function-vs-code-python?pivots=python-mode-decorators)
4. Azure API Management (consumption, DO NOT use developer)

<b>Authentication (Managed Identity) Roles Setup</b>:
- Azure Portal -> Function App -> Identity -> System Assigned -> On -> Add Azure Role Assignments
1. <b>Monitoring Metrics Publisher</b>: Ingest to Log Analytics (optional)
2. <b>Log Analytics Contributor</b>: Query Log Analytics
3. <b>Storage Queue Data Contributor</b>: Storage Queue Send/Get/Delete
4. <b>Storage Queue Data Message Processor</b>: Storage Queue Trigger for Azure Function
5. <b>Storage Blob Data Contributor</b>: Upload to Blob Storage
6. <b>Storage Table Data Contributor</b>: Logging

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/cb713394-ce13-4724-b567-ab9fe6afe99a)

<b>Environment Variables for Queue Triggers via Managed Identity</b>: 
- Setup via Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
1. <b>storageAccountConnectionString__queueServiceUri</b> -> https://<STORAGE_ACCOUNT>.queue.core.windows.net/
2. <b>storageAccountConnectionString__credential</b> -> managedidentity
3. <b>QueueQueryName</b> -> <STORAGE_QUEUE_NAME_FOR_QUERIES>
4. <b>QueueProcessName</b> -> <STORAGE_QUEUE_NAME_FOR_PROCESSING>

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/b0c4ce1c-affe-45bd-b9cf-f2db5ef398ae)

<b>Optional Environment Variables (reduces number of params in requests)</b>:
- Setup via Azure Portal -> Function App -> Settings -> Configuration -> Environment Variables
1. <b>QueueURL</b> -> <STORAGE_QUEUE_URL>
2. <b>TableURL</b> -> <STORAGE_TABLE_URL>
3. <b>TableIngestName</b> -> <STORAGE_TABLE_INGEST_LOG_NAME>
4. <b>TableQueryName</b> -> <STORAGE_TABLE_QUERY_LOG_NAME>
5. <b>TableProcessName</b> -> <STORAGE_TABLE_PROCESS_LOG_NAME>

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/9d2613e3-6d11-4069-88df-2fbef6db74bb)

<b>Azure Storage Setup</b>:
1. Create 1 container for data output files
   - <STORAGE_CONTAINER_NAME>
3. Create 4 queues for messages/jobs
   - <STORAGE_QUEUE_NAME_FOR_QUERIES>
   - <STORAGE_QUEUE_NAME_FOR_PROCESSING>
   - <STORAGE_QUEUE_NAME_FOR_QUERIES>-poison for failed messages
   - <STORAGE_QUEUE_NAME_FOR_PROCESSING>-poison for failed messages
5. Create 3 tables for logging (i.e. ingestlog, querylog, and processlog)
   - <STORAGE_TABLE_INGEST_LOG_NAME>
   - <STORAGE_TABLE_QUERY_LOG_NAME>
   - <STORAGE_TABLE_PROCESS_LOG_NAME>

<b>API Management (APIM) Setup:</b>
- Note: APIM is used to access the FastAPI Swagger/OpenAPI docs
1. Create APIM Service -> Consumption Pricing Tier (do not select developer) 
2. Add new API -> Function App 
   - Function App: <YOUR_FUNCTION>
   - Display Name: Protected API Calls
   - Name: protected-api-calls
   - Suffix: api
3. Remove all operations besides POST 
   - Edit POST operation 
      - Display name: azure_ingest_test_data
      - URL: POST /azure_ingest_test_data
   - Clone and Edit new POST operation 
      - Display name: azure_submit_query
      - URL: POST /azure_submit_query
   - Clone and Edit new POST operation 
      - Display name: azure_submit_query_parallel
      - URL: POST /azure_submit_query_parallel
   - Clone and Edit new POST operation 
      - Display name: azure_get_status_post
      - URL: POST /azure_get_status
   - Clone azure_get_status operation
      - Change from POST to GET
      - Display name: azure_get_status
      - URL: GET /azure_get_status
   - Edit OpenAPI spec json operation ids to match above
4. Add new API -> Function App
   - Function App: <YOUR_FUNCTION>
   - Display Name: Public Docs
   - Name: public-docs
   - Suffix: public
5. Remove all operations besides GET
   - Settings -> uncheck 'subscription required'
   - Edit GET operation
      - Display name: Documentation
      - URL: GET /docs
   - Clone and Edit new GET operation
      - Display name: OpenAPI Schema
      - URL: GET /openapi.json
   - Edit OpenAPI spec json operation ids to match above
   - Test at https://<APIM_ENDPOINT_NAME>.azure-api.net/public/docs

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/a98fc860-7bd7-4e15-9466-4863ddc1e18c)

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/765b9316-370d-49a4-a5d1-f29f1a701115)

<b>Queue Trigger Setup:</b>:
- To fix message encoding errors (default is base64), add "extensions": {"queues": {"messageEncoding": "none"}} to host.json
- Note: Failed messages/jobs are sent to <QUEUE_NAME>-poison

<b>Optional Data Collection Endpoint and Rule Setup for Log Analytics Ingest</b>:
1. Azure Portal -> Monitor -> Data Collection Endpoints -> Create
2. Azure Portal -> Log Analytics -> Table -> Create New Custom Table
3. Reference: [Tutorial: Send data to Azure Monitor Logs with Logs ingestion API (Azure portal)
](https://learn.microsoft.com/en-us/azure/azure-monitor/logs/tutorial-logs-ingestion-portal)

<b>Optional Security Settings</b>:
1. Restrict Azure Function App and APIM to specific IP address range(s)
- Networking -> Public Access -> Select Virtual Networks or IPs

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/a0c83909-4136-4f8b-a1c1-498d157cb4ba)

## Usage

Swagger UI Docs at https://<APIM_ENDPOINT_NAME>.azure-api.net/public/docs
- API calls require a APIM subscription key
- APIM -> Subscription -> Create Subscription -> Copy Key
- Paste in "Ocp-Apim-Subscription-Key" header field 

![image](https://github.com/dtagler/azure-log-analytics-data-export/assets/108005114/b8b0096a-9b7f-4318-bdd0-061e29a8826a)

1. <b>azure_submit_query()</b> or <b>azure_submit_query_parallel()</b>:

- HTTP POST Example:

```json
{
    "subscription_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "resource_group_name" : "XXXXXXXXXXXXXXXXXXXXXXX",
    "log_analytics_worksapce_name" : "XXXXXXXXXXXXXXXX",
    "log_analytics_workspace_id" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "storage_blob_url" : "https://XXXXXXXXXXXXXXXXXXXXX.blob.core.windows.net/",
    "storage_blob_container_name" : "XXXXXXXXXXXXX",
    "table_names_and_columns" : { "XXXXXXXXXXXXXXX_CL": ["TimeGenerated","DataColumn1","DataColumn2","DataColumn3","DataColumn4","DataColumn5","DataColumn6","DataColumn7","DataColumn8","DataColumn9"]},
    "start_datetime" : "2024-03-19 00:00:00",
    "end_datetime" : "2024-03-20 00:00:00"
}
```

- HTTP Response Examples:
    - azure_submit_query()
    - azure_submit_query_parallel()

```json
{
    "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "submit_status": "Success",
    "table_names": "XXXXXXXXXXX_CL",
    "start_datetime": "2024-03-19 00:00:00.000000",
    "end_datetime": "2024-03-20 00:00:00.000000",
    "total_row_count": 23000000,
    "subqueries_generated": 95,
    "subqueries_sent_to_queue": 95,
    "runtime_seconds": 92.1,
    "submit_datetime": "2024-03-26 16:24:38.771336"
}
```

```json
{
    "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "split_status": "Success",
    "table_names": "XXXXXXXXXXX_CL",
    "start_datetime": "2024-04-04 00:00:00.000000",
    "end_datetime": "2024-04-10 00:00:00.000000",
    "number_of_messages_generated": 6,
    "number_of_messages_sent": 6,
    "total_row_count": 2010000,
    "runtime_seconds": 0.9,
    "split_datetime": "2024-04-12 14:06:41.688752"
}
```

2. <b>azure_get_status()</b>:

- HTTP POST Request Example:
  
```json
{
    "query_uuid" : "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"
}
```

- HTTP Response Example:

```json
{
    "query_uuid": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
    "query_partitions" : 1,
    "submit_status": "Success",
    "processing_status": "Partial",
    "percent_complete": 29.5,
    "runtime_since_submit_seconds": 463.6
    "estimated_time_remaining_seconds": 1107.9,
    "number_of_subqueries": 95,
    "number_of_subqueries_success": 28,
    "number_of_subqueries_failed": 0,
    "query_row_count": 23000000,
    "output_row_count": 6972002,
    "output_file_size": 2.05,
    "output_file_units" : "GB"
}
```

## Issues

1. Azure Function App stops processing sub-queries, queue trigger not processing messages in queue:
   - Manually restart Azure Function App in Azure Portal
   - Use Premium or Dedicated Plan

2. Submit exceed 10 min Azure Function limit and fails
   - Use azure_submit_query_parallel() function 
   - Reduce the datetime range of the query (recommend less than 100M records)
   - Decrease break_up_query_freq value in azure_submit_query()
   - Decrease parallel_process_break_up_query_freq value in azure_submit_query_parallel()
   - Use Premium or Dedicated Plan with no time limit
  
3. Table row count values exceeding 2,147,483,647
   - Change type from int32 to int64

## Changelog

2.1.0:
- Updated azure queue triggers to use blueprints
- Added Zip Deployment 

2.0.0:
- Changed Azure Function code to use FastAPI in order to use Swagger UI
- Added pydantic input/output JSON schemas
- Updated documentation

1.5.0:
- Added azure_submit_queries() function for larger datetime ranges and parallel processing

1.4.0:
- Refactored code and made pylint edits
- Changed logging to % formatting from f-strings

1.3.1:
- Fixed UTC time zone bug
- Added estimated time remaining to get_status() response
- Added option to put storage queue and table params in env variables

1.3.0:
- Added pydantic input validation
- Added Open API yaml file for Azure API Management

1.2.0:
- Added get_status() azure function

1.1.0:
- Added logging to Azure Table Storage
- Added row count checks

1.0.0:
- Initial release

## References

1. [How to use logic apps to handle large amounts of data from log analtyics](https://techcommunity.microsoft.com/t5/azure-integration-services-blog/how-to-use-logic-apps-to-handle-large-amount-of-data-from-log/ba-p/2797466)
2. [FastAPI on Azure Functions](https://blog.pamelafox.org/2022/11/fastapi-on-azure-functions-with-azure.html)
