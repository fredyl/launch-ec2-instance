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


def fetch_data_from_url(pagination_urls):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(f"https://customer-experience-api.arifleet.com/v1/{pagination_urls}", headers=headers)
    if response.status_code != 200:

fetch_data_udf = udf(lambda url: fetch_data_from_url(pagination_urls), StringType())


#Check if data is available for the given type and key before generating URLs
def check_data_availability(token, data_type, code_key, code_value): 
    endpoint = f"{data_type}?{code_key}={code_value}" 
    _, response_data = get_holman_api_response(token, endpoint)

    if response_data:
        total_records = response_data.get("totalRecords", 0)
        print(f"Total records available: {total_records} for {data_type} with {code_key}={code_value}")
        return total_records > 0
    else:
        print(f"No data available for {data_type} with {code_key}={code_value}")



# Configuration for endpoints with corresponding data keys
holman_coded_endpoints = [
    {"data_type": "billing", "code_key": "billingTypeCode", "data_key": "billing", "primary_key": "invoiceNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "can", "primary_key": "clientVehicleNumber"},
    {"data_type": "fuels", "code_key": "transDateCode", "data_key": "us", "primary_key": "usRecordID"},
    {"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"}
]

# Loop to fetch and upsert data for each endpoint and code value
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    
    for code_value in range(1, 4):  # Looping through code_key values 1 to 3

         # Preliminary check to see if data is available
        if not check_data_availability(token, data_type, code_key, code_value):
            print(f"No data found for {data_type} with code key {code_key} = {code_value}. Skipping...")
            continue  # Skip to the next code_value if no data is available

        batch_urls, loops =generate_paginated_urls(token, data_key, code_key, code_value, batch_size=200)
        batch_df = spark.createDataFrame(batch_urls)
        batch_df = batch_df.withColumn("part", floor(col("pageNumber") / 10)).repartition(loops, "part")


        output_df = batch_df.withColumn("data", fetch_data_udf(col("url"))).cache()

        # total_pages = get_all_pages(token="your_token", endpoint=f"{data_key}?{code_key}={code_value}")
        # pagination_urls = generate_paginated_urls("your_token", data_key, code_key, code_value, batch_size=200)
        
        data_list = []
        for row in output_df.collect():
            if row.data is not None and row.data != "Bad Response":
                try:
                    data_list.extend(json.loads(row.data)[data_key])
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON for URL: {row.urpageNumberl}: {e}")
            else:
                print(f"skipping page {row.pageNumber} due to None or Bad Response")

        if data_list:
            cleaned_data = replace_null_values(data_list)

            Holman_Upsert_data(data_type, data_key, cleaned_data, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
        else:
            print(f"No data found for data type {data_type} with code value {code_value}")



Total records available: 0 for billing with billingTypeCode=1
No data found for billing with code key billingTypeCode = 1. Skipping...
Total records available: 0 for billing with billingTypeCode=2
No data found for billing with code key billingTypeCode = 2. Skipping...
Total records available: 0 for billing with billingTypeCode=3
No data found for billing with code key billingTypeCode = 3. Skipping...
Total records available: 0 for fuels with transDateCode=1
No data found for fuels with code key transDateCode = 1. Skipping...
Total records available: 0 for fuels with transDateCode=2
No data found for fuels with code key transDateCode = 2. Skipping...
Total records available: 0 for fuels with transDateCode=3
No data found for fuels with code key transDateCode = 3. Skipping...
Total records available: 0 for fuels with transDateCode=1
No data found for fuels with code key transDateCode = 1. Skipping...
