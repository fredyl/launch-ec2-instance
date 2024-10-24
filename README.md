Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Checkpoint Updated to 24
Cancelled


def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=100):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
  
    
    total_pages = get_total_pages(data_type, code_key, code, token)
    while page <= total_pages:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            return data_list  # Return the data list after fetching a batch
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)

        if response.status_code == 200:
            batch_data = response_data.get(data_key)

            if batch_data is None or len(batch_data) == 0:
                print(f"No data found for {data_type}_{data_key} on page {page}, stopping pagination")
                save_checkpoint(endpoint_key, page)
                break

            print(f"Processing page {page}, with {len(batch_data)} records")
            data_list.extend(batch_data)
            page += 1
            pages_fetched += 1
        else:
            print(f"Error: {response.status_code} {response.text}")
            break

    # Save checkpoint if loop completes
    save_checkpoint(endpoint_key, page)
    return data_list



import concurrent.futures


#define a set of endpoints with corresponding data keys
holman_coded_endpoints =[
    # {
    #     "data_type" : "billing",
    #     "code_key": "billingTypeCode",
    #     "data_key": "billing",
    #     "primary_key": "invoiceNumber"
    #  },
    # {
    #     "data_type": "fuels",
    #     "code_key": "transDateCode",
    #     "data_key": "can",
    #     "primary_key": "clientVehicleNumber"
    # },
    {
        "data_type": "fuels",
        "code_key": "transDateCode",
        "data_key": "us",
        "primary_key": "usRecordID"
    },
    # {
    #     "data_type" : "violation",
    #     "code_key": "violationDateCode",
    #     "data_key": "violations",
    #     "primary_key": "record_id"
    # }
]

# Function to process each endpoint
def process_endpoint(endpoint_config, token):
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    code = 1
    last_page = get_last_checkpoint(f"Holman_{data_type}_{code_key}_{code}", default=1)
    total_pages = 2808  # Set this to the actual total number of pages you expect

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_page = {
            executor.submit(fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=100)): page
            for page in range(last_page, total_pages + 1)
        }
        

        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            try:
                data_list = future.result()
                if data_list:
                    clean_data_list = replace_null_values(data_list)
                    Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
                else:
                    print(f"No data found for {data_type}_{data_key} on page {page}")
            except Exception as exc:
                print(f"Page {page} generated an exception: {exc}")
# Iterate through the endpoints to fetch and upsert data in parallel
for endpoint_config in holman_coded_endpoints:
    process_endpoint(endpoint_config, token)
