
having the below errors when running

holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "invoiceNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"}
]

# Main execution loop with UDF-based parallel data fetching
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Loop through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
        
        # Retrieve total pages and skip if no pages are available
        total_pages = get_all_pages("your_token", data_type, code_key, code_value)
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 10)).repartition(loops, "part")

        # Apply the UDF to fetch data for each URL
        results_df = batch_df.withColumn("data", fetch_data_udf(col("url")))
        
        # Generate a list of URLs for each page
        # urls = [f"https://customer-experience-api.arifleet.com/v1/{data_type}?{code_key}={code_value}&pageNumber={page}" 
        #         for page in range(1, total_pages + 1)]
        # # print(urls)

        # # Create a DataFrame from the URLs list
        # urls_df = spark.createDataFrame([(url,) for url in urls], ["url"])
        # # urls_df.show()
        
        
        # results_df.show()
        
        # Collect and process each page's data
        data_list = []
        for row in results_df.collect():
            if row.data:
                try:
                    json_data = json.loads(row.data)
                    if isinstance(json_data, dict):
                        page_data = json_data.get(data_key, [])
                        data_list.extend(page_data)
                    elif isinstance(json_data, list):
                        data_list.extend(json_data)
                    else:
                        print("Expected a dictionary but got a list.")
                except json.JSONDecodeError:
                    print("Failed to parse JSON response.")


        # Upsert data if available
        if data_list:
            cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data to upsert for data type {data_type} with code value {code_value}")



PythonException: 
  An exception was thrown from the Python worker. Please see the stack trace below.
Traceback (most recent call last):
  File "/root/.ipykernel/1348/command-1723439101937605-2048933269", line 15, in <lambda>
  File "/root/.ipykernel/1348/command-1723439101937605-2048933269", line 6, in fetch_data_from_url
  File "/databricks/python/lib/python3.11/site-packages/requests/api.py", line 73, in get
    return request("get", url, params=params, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python/lib/python3.11/site-packages/requests/api.py", line 59, in request
    return session.request(method=method, url=url, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python/lib/python3.11/site-packages/requests/sessions.py", line 575, in request
    prep = self.prepare_request(req)
           ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/databricks/python/lib/python3.11/site-packages/requests/sessions.py", line 486, in prepare_request
    p.prepare(
  File "/databricks/python/lib/python3.11/site-packages/requests/models.py", line 368, in prepare
    self.prepare_url(url, params)
  File "/databricks/python/lib/python3.11/site-packages/requests/models.py", line 439, in prepare_url
    raise MissingSchema(
requests.exceptions.MissingSchema: Invalid URL 'billing?billingTypeCode&pageNumber=1': No scheme supplied. Perhaps you meant https://billing?billingTypeCode&pageNumber=1?
File <command-1723439101937788>, line 45
     31 # Generate a list of URLs for each page
     32 # urls = [f"https://customer-experience-api.arifleet.com/v1/{data_type}?{code_key}={code_value}&amp;pageNumber={page" target="_blank" rel="noopener noreferrer">https://customer-experience-api.arifleet.com/v1/{data_type}?{code_key}={code_value}&amp;pageNumber={page</a>}&quot; 
     33 #         for page in range(1, total_pages + 1)]
   (...)
     42 
     43 # Collect and process each page's data
     44 data_list = []
---> 45 for row in results_df.collect():
     46     if row.data:
     47         try:
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
