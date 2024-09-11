def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):
    """
    Get object ids for each object type in Power BI api
    """
     
    group_ids = all_group_ids[0]
    powerbi_object_Ids = []
    response_json = []

    for group_id in group_ids:
        url = base_url + f"/groups/{group_id}/{object_type}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            items = response.json().get('value', [])
            response_json.append(response.json())
            for item in items:
                if key_id in item:
                    powerbi_object_Ids.append(item[key_id])
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
    
    return response_json, powerbi_object_Ids


def call_power_bi_api_for_all_data(access_token, data_ids_dict, api_name, item_type):
    """
    the function makes a generic API call for each item in each group.
    """
    
    all_data = []
    skipped_data = []

#Looping through each group ID and its corresponding list of item IDs
    for group_id, item_ids in data_ids_dict.items():

        if not isinstance(item_ids, list):
            raise Exception(f"API response must be a list, but got type {type(item_ids)}")

        for item_id in item_ids:
            if not isinstance(item_id, str):
                raise Exception(f"API response must be a list, but got type {type(item_id)}")

            for item_id in item_ids:
                if item_id is None:
                    print(f"Skipping item with no id for group id {group_id}")
                    continue
            
            #Creating the API endpoint for the specific group, item type, and item ID
            endpoint=f"groups/{group_id}/{item_type}/{item_id}/{api_name}"

            #Call API and print the response
            try: 
                data = call_powerbi_api(access_token, endpoint)
                if isinstance(data, list):
                    all_data.extend(data)
                elif isinstance(data, dict):
                    all_data.append(data)
            except Exception as e:
                skipped_data.append(item_id)
                continue
        

    return all_data





    import requests

Processing group_id e26b065e-851e-4fc4-95da-4600e0f52423
Processing group_id 3b1b45fe-32cd-4bbb-afa8-005c505e6f6a
exception while intercepting server message: {'jsonrpc': '2.0', 'method': 'textDocument/didChange', 'params': {'cellRanges': [{'startLine': 0, 'stopLine': 3, 'commandId': 1825532333264041}, {'startLine': 3, 'stopLine': 5, 'commandId': 1825532333264042}, {'startLine': 5, 'stopLine': 21, 'commandId': 1825532333264043}, {'startLine': 21, 'stopLine': 28, 'commandId': 1825532333264044}, {'startLine': 28, 'stopLine': 44, 'commandId': 1825532333264045}, {'startLine': 44, 'stopLine': 62, 'commandId': 2798623019031649}, {'startLine': 62, 'stopLine': 68, 'commandId': 1825532333264046}, {'startLine': 68, 'stopLine': 93, 'commandId': 2798623019031959}, {'startLine': 93, 'stopLine': 116, 'commandId': 1825532333264047}, {'startLine': 116, 'stopLine': 145, 'commandId': 1825532333264048}, {'startLine': 145, 'stopLine': 187, 'commandId': 2798623019031653}, {'startLine': 187, 'stopLine': 218, 'commandId': 2798623019031569}, {'startLine': 218, 'stopLine': 265, 'commandId': 1825532333264049}, {'startLine': 265, 'stopLine': 279, 'commandId': 1825532333264050}, {'startLine': 279, 'stopLine': 280, 'commandId': 1825532333264052}], 'contentChanges': [{'text': 'from databricks.sdk.runtime import display, displayHTML, getArgument, sc, spark, sql, sqlContext, table, udf;from dbruntime.dbutils import DBUtils as dbutils;_sqldf=None;get_ipython=None;In=None;Out=None;_ih=None;_oh=None;_dh=None;_=None;__=None;___=None;_i=None;_ii=None;_iii=None\n# importing libaries from tglib\nfrom tglib import ErrorHandler\n#sends out email to distrubution group\neh = ErrorHandler(sendEmail = False)\nimport requests, msal, json,time, datetime\nimport concurrent.futures\nfrom datetime import datetime\nfrom pyspark.sql import SparkSession, DataFrame\nfrom pyspark.sql.functions import col,cast, lit, when, monotonically_increasing_id\nfrom pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType\n\nclient_id = "dcb4b38c-e50a-49ec-a5a3-d5300184e4be"\ntenant_id = "aadec23e-c40c-47d7-a647-2566ee1c67da"\nclient_secret = dbutils.secrets.get(scope="BIHubScope",key="databricks-powerbi-connector")\nauthority = f\'https://login.microsoftonline.com/{tenant_id}\'\nscope = [\'https://analysis.windows.net/powerbi/api/.default\']\n\n\n\n\napp = msal.ConfidentialClientApplication(\n        client_id,\n        authority=f"https://login.microsoftonline.com/{tenant_id}",\n        client_credential=client_secret\n    )\ntoken_response = app.acquire_token_for_client(scopes=[\'https://analysis.windows.net/powerbi/api/.default\'])\naccess_token = token_response[\'access_token\']\n# def call_powerbi_api(access_token, endpoint, params=None):\n#     headers= { \'Authorization\': f\'Bearer {access_token}\',\n#                 \'Content-Type\': \'application/json\'\n#         }\n#     base_url = \'https://api.powerbi.com/v1.0/myorg/\'\n#     url = base_url + endpoint\n#     for _ in range(3):\n#         response = requests.get(url, headers=headers, params=params)\n#         if response.status_code == 200:\n#             json_data = response.json()\n#             return json_data, response\n#         elif response.status_code == 429:\n#             retry_after = int(response.headers.get(\'Retry-After\', 8))\n#             time.sleep(retry_after)\n#         else:\n#             raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")\ndef call_powerbi_api(access_token, endpoint, params=None):\n\n    url = base_url + endpoint\n    for _ in range(3):\n        response = requests.get(url, headers=headers, params=params)\n        if response.status_code == 200:\n            data = response.json()\n            return data[\'value\'] if \'value\' in data else data\n        elif response.status_code == 429:\n            retry_after = int(response.headers.get(\'Retry-After\', 8))\n            time.sleep(retry_after)\n        else:\n            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")\n\nheaders= { \'Authorization\': f\'Bearer {access_token}\',\n                \'Content-Type\': \'application/json\'\n        }\nbase_url = \'https://api.powerbi.com/v1.0/myorg/\'\ndef get_all_groups_ids(access_token):\n    groups_data = call_powerbi_api(access_token, \'groups\')\n    group_ids = [groups_data[\'id\'] for groups_data in groups_data]\n    return group_ids, groups_data\n\nall_group_ids = get_all_groups_ids(access_token)\ndef get_all_object_id_for_each_object_type(access_token, object_type, key_id): \n    """ Get object ids for each object type in Power BI API. """ \n    \n    group_ids = all_group_ids[0] \n    powerbi_object_Ids = [] \n    response_json = []\n\n    for group_id in group_ids:\n        url = base_url + f"/groups/{group_id}/{object_type}"\n        try:\n            response = requests.get(url, headers=headers)\n            response.raise_for_status() \n            data = response.json()\n            response_json.append(data)\n            items = data.get(\'value\', [])\n            powerbi_object_Ids.extend(item[key_id] for item in items if key_id in item)\n        except requests.RequestException as e:\n            print(f"Request failed for group_id {group_id} with error: {e}")\n        except Exception as e:\n\n            print(f"An error occurred: {e}")\n\n    return response_json, powerbi_object_Ids\n\nprint(get_all_object_id_for_each_object_type(access_token, "datasets", "id")[1])\n# def get_all_object_id_for_each_object_type(access_token, object_type, key_id ):\n#     """\n#     Get object ids for each object type in Power BI api\n#     """\n     \n#     group_ids = all_group_ids[0]\n#     powerbi_object_Ids = []\n#     response_json = []\n\n#     for group_id in group_ids:\n#         url = base_url + f"/groups/{group_id}/{object_type}"\n#         response = requests.get(url, headers=headers)\n#         if response.status_code == 200:\n#             items = response.json().get(\'value\', [])\n#             response_json.append(response.json())\n#             for item in items:\n#                 if key_id in item:\n#                     powerbi_object_Ids.append(item[key_id])\n#         else:\n#             raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")\n    \n#     return response_json, powerbi_object_Ids\n\ndef get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):\n    """\n    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the\n    object_id\'s\n    """\n    group_ids = all_group_ids[0]\n    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]\n\n    items_llist = []\n   \n    for group_id in group_ids:\n        print(f"Processing group_id {group_id}")\n        for object_id in object_ids:\n            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"\n            response = requests.get(url, headers=headers)\n            if response.status_code == 200:\n                if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object\n                    items = response.json()\n                    items[key_id] = object_id\n                    items_llist.append(items)\n                else:\n                    items = response.json().get(\'value\', [])\n                    for item in items:\n                        item[key_id] = object_id\n                    items_llist.extend(items)\n    return items_llist\n    \n    print(get_object_type_items(access_token, "datasets", "id", "refreshSchedule"))\n    \n# import concurrent.futures\n\n# def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):\n#     """\n#     retrieves items from object_type (e.g. datasets, reports, dataflows) based on the\n#     object_id\'s\n#     """\n#     group_ids = all_group_ids[0]\n#     object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]\n\n#     items_llist = []\n\n#     def process_object(group_id, object_id):\n#         url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"\n#         response = requests.get(url, headers=headers)\n#         if response.status_code == 200:\n#             if sub_api_endpoint == "refreshSchedule":\n#                 items = response.json()\n#                 items[key_id] = object_id\n#                 return items\n#             else:\n#                 items = response.json().get(\'value\', [])\n#                 for item in items:\n#                     item[key_id] = object_id\n#                 return items\n#         return []\n\n#     with concurrent.futures.ThreadPoolExecutor() as executor:\n#         futures = []\n#         for group_id in group_ids:\n#             print(f"Processing group_id {group_id}")\n#             for object_id in object_ids:\n#                 futures.append(executor.submit(process_object, group_id, object_id))\n\n#         for future in concurrent.futures.as_completed(futures):\n#             items = future.result()\n#             if items:\n#                 items_llist.extend(items)\n\n#     return items_llist\n\n\ndef get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):\n#     """\n#     retrieves items from object_type (e.g. datasets, reports, dataflows) based on the\n#     object_id\'s\n#     """\n \n#     group_ids = all_group_ids[0]\n#     object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]\n\n#     items_llist = []\n   \n#     for group_id in group_ids:\n#         print(f"Processing group_id {group_id}")\n#         for object_id in object_ids:\n#             endpoint = f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"\n#             print(endpoint)\n#             jason_data, response = call_powerbi_api(access_token, endpoint)\n#             if response == 200:\n#                 if sub_api_endpoint == "refreshSchedule": #sub_api_endpoint is refreshSchedule the process since the response is a json object\n#                     items = jason_data.json()\n#                     items[key_id] = object_id\n#                     items_llist.append(items)\n#                 else:\n#                     items = jason_data.json().get(\'value\', [])\n#                     for item in items:\n#                         item[key_id] = object_id\n#                     items_llist.extend(items)\n#     return items_llist\n    \n# print(json.dumps(get_object_type_items(access_token, "datasets", "id", "refreshSchedule"), indent=2))\n\ndef create_dataframe_and_update_or_merge_table(access_token, table_name, primary_key, api_flag=None, object_type=None, key_id=None, sub_api_endpoint=None):\n\n     print("create_dataframe_and_update_or_merge_tablepe")\n    """\n    create Dataframe from json data and update or merge the data to delta table\n    api_flag (e.g groups)\n    """\n\n    #Fetch the current timetamp\n    timestamp = datetime.utcnow().isoformat()\n\n    if api_flag is not None:\n        json_object = get_all_groups_ids(access_token)[1]\n\n    else:\n        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)\n    \n    json_string = json.dumps(json_object)\n    json_rdd = spark.sparkContext.parallelize([json_string])\n    spark_df = spark.read.option("multiLine", True).json(json_rdd)\n    primary_key = spark_df[primary_key]\n\n    all_data_df = spark_df\n\n    #Add LastModified column to the dataframe\n    spark_df = spark_df.withColumn("LastModified", lit(timestamp))\n    \n    if spark.catalog.tableExists(table_name):\n        print(f"Table {table_name} exists, updating")\n        existing_df = spark.table(table_name)\n        all_data_df.createOrReplaceTempView(\'new_data\')\n        \n        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])\n\n        merge_query = f"""\n        MERGE INTO {table_name} AS t\n        USING new_data AS n\n        ON {merge_condition}\n        WHEN MATCHED THEN \n        UPDATE SET *\n        WHEN NOT MATCHED\n         THEN INSERT *\n        """\n    else: \n        print(f"Table {table_name} does not exist. Creating a new table.")\n        spark_df.write.format("delta").saveAsTable(table_name)\n\n\n#getting data and creating or updataing the table for datasets refreshes\nprint("Running datasets refreshes Power BI API Call")\ncreate_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshes", primary_key=["requestId"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshes")\n\n\n# # #getting data and creating or updataing the table for dataflows transactions\n# print("Running dataflows transactions Power BI API Call")\n# create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_dataflows_transactions", primary_key=["id"],api_flag=None, object_type="dataflows", key_id="objectId", sub_api_endpoint="transactions")\n\n\n# #getting data and creating or updataing the table for datasets refreshschedule\n# print("Running datasets refreshschedule Power BI API Call")\n# create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshschedule", primary_key=["days"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshSchedule")\n\n'}], 'cursorPosition': {'line': 143, 'character': 4}, 'textDocument': {'uri': '/notebook/3b9145ca-3aea-45df-886f-a11f32df1b1d/3383306747623278', 'version': 227}}}
Traceback (most recent call last):
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/base.py", line 47, in intercept_message_to_server_safe
    return self.intercept_message_to_server(msg)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 120, in intercept_message_to_server
    self._handle_text_document_did_change(uri, msg)
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 101, in _handle_text_document_did_change
    self._sanitize_line_magics_and_set_document(doc, uri)
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 134, in _sanitize_line_magics_and_set_document
    sanitized_text = sanitize_document_line_magics(text)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python_shell/dbruntime/lsp_backend/LineMagicSanitizerTransformer.py", line 61, in sanitize_document_line_magics
    tokens = make_tokens_by_line(lines)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python/lib/python3.11/site-packages/IPython/core/inputtransformer2.py", line 535, in make_tokens_by_line
    for token in tokenutil.generate_tokens_catch_errors(
  File "/databricks/python/lib/python3.11/site-packages/IPython/utils/tokenutil.py", line 36, in generate_tokens_catch_errors
    for token in tokenize.generate_tokens(readline):
  File "/usr/lib/python3.11/tokenize.py", line 516, in _tokenize
    raise IndentationError(
  File "<tokenize>", line 222
    """
IndentationError: unindent does not match any outer indentation level
exception while intercepting server message: {'jsonrpc': '2.0', 'method': 'textDocument/didChange', 'params': {'cellRanges': [{'startLine': 0, 'stopLine': 3, 'commandId': 1825532333264041}, {'startLine': 3, 'stopLine': 5, 'commandId': 1825532333264042}, {'startLine': 5, 'stopLine': 21, 'commandId': 1825532333264043}, {'startLine': 21, 'stopLine': 28, 'commandId': 1825532333264044}, {'startLine': 28, 'stopLine': 44, 'commandId': 1825532333264045}, {'startLine': 44, 'stopLine': 62, 'commandId': 2798623019031649}, {'startLine': 62, 'stopLine': 68, 'commandId': 1825532333264046}, {'startLine': 68, 'stopLine': 93, 'commandId': 2798623019031959}, {'startLine': 93, 'stopLine': 116, 'commandId': 1825532333264047}, {'startLine': 116, 'stopLine': 145, 'commandId': 1825532333264048}, {'startLine': 145, 'stopLine': 187, 'commandId': 2798623019031653}, {'startLine': 187, 'stopLine': 218, 'commandId': 2798623019031569}, {'startLine': 218, 'stopLine': 265, 'commandId': 1825532333264049}, {'startLine': 265, 'stopLine': 279, 'commandId': 1825532333264050}, {'startLine': 279, 'stopLine': 280, 'commandId': 1825532333264052}], 'contentChanges': [{'text': 'from databricks.sdk.runtime import display, displayHTML, getArgument, sc, spark, sql, sqlContext, table, udf;from dbruntime.dbutils import DBUtils as dbutils;_sqldf=None;get_ipython=None;In=None;Out=None;_ih=None;_oh=None;_dh=None;_=None;__=None;___=None;_i=None;_ii=None;_iii=None\n# importing libaries from tglib\nfrom tglib import ErrorHandler\n#sends out email to distrubution group\neh = ErrorHandler(sendEmail = False)\nimport requests, msal, json,time, datetime\nimport concurrent.futures\nfrom datetime import datetime\nfrom pyspark.sql import SparkSession, DataFrame\nfrom pyspark.sql.functions import col,cast, lit, when, monotonically_increasing_id\nfrom pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType, ArrayType\n\nclient_id = "dcb4b38c-e50a-49ec-a5a3-d5300184e4be"\ntenant_id = "aadec23e-c40c-47d7-a647-2566ee1c67da"\nclient_secret = dbutils.secrets.get(scope="BIHubScope",key="databricks-powerbi-connector")\nauthority = f\'https://login.microsoftonline.com/{tenant_id}\'\nscope = [\'https://analysis.windows.net/powerbi/api/.default\']\n\n\n\n\napp = msal.ConfidentialClientApplication(\n        client_id,\n        authority=f"https://login.microsoftonline.com/{tenant_id}",\n        client_credential=client_secret\n    )\ntoken_response = app.acquire_token_for_client(scopes=print("create_dataframe_and_update_or_merge_tablepe")\n    """\n    create Dataframe from json data and update or merge the data to delta table\n    api_flag (e.g groups)\n    """\n\n    #Fetch the current timetamp\n    timestamp = datetime.utcnow().isoformat()\n\n    if api_flag is not None:\n        json_object = get_all_groups_ids(access_token)[1]\n\n    else:\n        json_object = get_object_type_items(access_token, object_type, key_id, sub_api_endpoint)\n    \n    json_string = json.dumps(json_object)\n    json_rdd = spark.sparkContext.parallelize([json_string])\n    spark_df = spark.read.option("multiLine", True).json(json_rdd)\n    primary_key = spark_df[primary_key]\n\n    all_data_df = spark_df\n\n    #Add LastModified column to the dataframe\n    spark_df = spark_df.withColumn("LastModified", lit(timestamp))\n    \n    if spark.catalog.tableExists(table_name):\n        print(f"Table {table_name} exists, updating")\n        existing_df = spark.table(table_name)\n        all_data_df.createOrReplaceTempView(\'new_data\')\n        \n        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])\n\n        merge_query = f"""\n        MERGE INTO {table_name} AS t\n        USING new_data AS n\n        ON {merge_condition}\n        WHEN MATCHED THEN \n        UPDATE SET *\n        WHEN NOT MATCHED\n         THEN INSERT *\n        """\n    else: \n        print(f"Table {table_name} does not exist. Creating a new table.")\n        spark_df.write.format("delta").saveAsTable(table_name)\n\n\n#getting data and creating or updataing the table for datasets refreshes\nprint("Running datasets refreshes Power BI API Call")\ncreate_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshes", primary_key=["requestId"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshes")\n\n\n# # #getting data and creating or updataing the table for dataflows transactions\n# print("Running dataflows transactions Power BI API Call")\n# create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_dataflows_transactions", primary_key=["id"],api_flag=None, object_type="dataflows", key_id="objectId", sub_api_endpoint="transactions")\n\n\n# #getting data and creating or updataing the table for datasets refreshschedule\n# print("Running datasets refreshschedule Power BI API Call")\n# create_dataframe_and_update_or_merge_table(access_token=access_token, table_name="bronze.pbi_datasets_refreshschedule", primary_key=["days"],api_flag=None, object_type="datasets", key_id="id", sub_api_endpoint="refreshSchedule")\n\n'}], 'cursorPosition': {'line': 143, 'character': 0}, 'textDocument': {'uri': '/notebook/3b9145ca-3aea-45df-886f-a11f32df1b1d/3383306747623278', 'version': 228}}}
Traceback (most recent call last):
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/base.py", line 47, in intercept_message_to_server_safe
    return self.intercept_message_to_server(msg)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 120, in intercept_message_to_server
    self._handle_text_document_did_change(uri, msg)
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 101, in _handle_text_document_did_change
    self._sanitize_line_magics_and_set_document(doc, uri)
  File "/databricks/python_shell/dbruntime/lsp_backend/middleware/document_syncing.py", line 134, in _sanitize_line_magics_and_set_document
    sanitized_text = sanitize_document_line_magics(text)
                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python_shell/dbruntime/lsp_backend/LineMagicSanitizerTransformer.py", line 61, in sanitize_document_line_magics
    tokens = make_tokens_by_line(lines)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python/lib/python3.11/site-packages/IPython/core/inputtransformer2.py", line 535, in make_tokens_by_line
    for token in tokenutil.generate_tokens_catch_errors(
  File "/databricks/python/lib/python3.11/site-packages/IPython/utils/tokenutil.py", line 36, in generate_tokens_catch_errors
    for token in tokenize.generate_tokens(readline):
  File "/usr/lib/python3.11/tokenize.py", line 516, in _tokenize
    raise IndentationError(
  File "<tokenize>", line 222
    """
IndentationError: unindent does not match any outer indentation level
Processing group_id 9b884ac0-d5bb-41a2-9c35-32f2a47e004b
Processing group_id db79f37e-9671-41e2-af91-3f3c751e073d

