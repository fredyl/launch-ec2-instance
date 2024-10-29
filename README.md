for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    
    for code_value in range(1, 4):  # Looping through code_key values 1 to 3
        print(f"Fetching data for {data_type} with code key {code_key} = {code_value}")
        
        # Construct the endpoint based on `data_type`, `code_key`, and `code_value`
        endpoint = f"{data_type}?{code_key}={code_value}"
        
        # Get the total pages for pagination
        total_pages = get_all_pages(token="your_token", endpoint=endpoint)
        
        if total_pages > 0:
            data_list = []
            for page_num in range(1, total_pages + 1):
                print(f"Fetching page {page_num} for {data_type} with code {code_value}")
                paginated_endpoint = f"{endpoint}&page={page_num}"
                _, response_data = get_holman_api_response("your_token", paginated_endpoint)
                
                if response_data and data_key in response_data:
                    data_list.extend(response_data[data_key])

            if data_list:
                cleaned_data = replace_null_values(data_list)
                Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
                print(f"Data upserted for data type {data_type} with code value {code_value}")
            else:
                print(f"No data found for data type {data_type} with code value {code_value}")
        else:
            print(f"No pages to fetch for {data_type} with code value {code_value}")
