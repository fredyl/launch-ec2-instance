def get_holman_api_response(token, endpoint, param_pagination = None):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    # headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers, params=param_pagination)
    if response.status_code == 204:
        print(f"Received 204, No Content from the API.")
        return response, None
    elif response.status_code == 200:
        print(f"Received 200, Success from the API.")
        response_data = response.json()
        return response, response_data
    elif response.status_code == 401:
        token = get_token()
        time.sleep(5)
        return get_holman_api_response(token, endpoint)
    else:
        raise Exception("Failed:", response.status_code, response.text)


        def get_all_pages(token,data_type, code_key, code_value):
    _, response_data  = get_holman_api_response(token, endpoint)
    if response_data:
        total_pages = int(response_data.get("totalPages", 1))
        print(f"Total pages: {total_pages}")
        return total_pages
    else:
        print("No pages found")

# Generates paginated URLs based on the total number of pages
def generate_paginated_urls(token, data_key, code_key, code_value, batch_size=100):
    total_pages = get_all_pages(token, data_type, code_key, code_value)
    if total_pages == 0:
        raise Exception("No pages found")

    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    print(f" loops : {loops}")

    for l in range(loops):
        page = l + 1
        url = f"{data_key}?{code_key}&pageNumber={page}"
        pagination_urls.append({"pageNumber": page, "url": url})
        print(pagination_urls)

    return pagination_urls, loops




# Data upsert function for Delta table
def Holman_Upsert_data(data_type, data_key, data_list, primary_key):
    table_name = f"bronze.holman_{data_type}_{data_key}"
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    df.display()


def fetch_data_from_url(pagination_urls):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(pagination_urls, headers=headers)
    if response.status_code == 200:
        return response.text
    return None

fetch_data_udf = udf(lambda url: fetch_data_from_url(pagination_urls), StringType())

def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list


#Check if data is available for the given type and key before generating URLs
def check_data_availability(token, data_type, code_key, code_value): 
    endpoint = f"{data_type}?{code_key}={code_value}" 
    _, response_data = get_holman_api_response(token, endpoint)

    if response_data:
        total_pages = int(response_data.get("totalPaages", 0))
        print(f"Total records available: {total_pages} for {data_type} with {code_key}={code_value}")
        return total_pages > 0
    else:
        print(f"No data available for {data_type} with {code_key}={code_value}")
        return False



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

        # Generate a list of URLs for each page
        urls = [f"https://customer-experience-api.arifleet.com/v1/{data_type}?{code_key}={code_value}&pageNumber={page}" 
                for page in range(1, total_pages + 1)]
        print(urls)

        # Create a DataFrame from the URLs list
        urls_df = spark.createDataFrame([(url,) for url in urls], ["url"])
        urls_df.show()
        
        # Apply the UDF to fetch data for each URL
        results_df = urls_df.withColumn("data", fetch_data_udf(col("url")))
        results_df.show(truncate=False)
        
        # Collect and process each page's data
        data_list = []
        for row in results_df.collect():
            if row.data:
                try:
                    page_data = json.loads(row.data).get(data_key, [])
                    data_list.extend(page_data)
                except json.JSONDecodeError:
                    print("Failed to parse JSON response.")

        # Upsert data if available
        if data_list:
            cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data to upsert for data type {data_type} with code value {code_value}")

    
        
