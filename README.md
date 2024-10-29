# Main execution loop for fetching and processing data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]

    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"\nChecking data for {data_type} with code key {code_key} = {code_value}")
        
        # Retrieve total pages and skip if no pages are available
        total_pages = get_total_pages("your_token", data_type, code_key, code_value)
        if total_pages == 0:
            print(f"No pages available for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue

        # Loop through pages and fetch data
        data_list = []
        for page_num in range(1, total_pages + 1):
            print(f"Fetching page {page_num} for {data_type} with code {code_value}")
            params = {"pageNumber": page_num}
            paginated_endpoint = f"{data_type}?{code_key}={code_value}"
            
            _, response_data = get_holman_api_response("your_token", paginated_endpoint, params=params)
            if response_data and data_key in response_data:
                page_data = response_data.get(data_key, [])
                if page_data:
                    data_list.extend(page_data)
                    print(f"Fetched {len(page_data)} records from page {page_num}.")
                else:
                    print(f"No data found on page {page_num}.")
            else:
                print(f"Failed to fetch data on page {page_num} or key '{data_key}' not in response.")

        # Upsert data if available
        if data_list:
            cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data to upsert for data type {data_type} with code value {code_value}")
