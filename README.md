
holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "vehicleNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    # {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "clientVehicleNumber"},
    # {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "violationNo"}
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
        total_pages = get_all_pages(token, data_type, code_key, code_value)
        print(f"Total pages for {code_key}_{code_value}: {total_pages}")
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 100)).repartition(loops, "part")
        # batch_df.show()

        # Apply the UDF to fetch data for each URL
        result_df = batch_df.withColumn("data", fetch_data_udf(col("url")))
        # result_df.show()

        # Collect and process each page's data
        data_list = []
        for row in result_df.collect():
            if row.data and not row.data.startswith("Error") and not row.data.startswith("Exception"):
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
            else:
                print(f"Error fetching data for URL: {row.url}, Response: {row.data}")


        # Upsert data if available
        if data_list:
            cleaned_data = replace_null_values(data_list)
            # print(cleaned_data)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data to upsert for data type {data_type} with code value {code_value}")
