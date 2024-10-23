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
    code = 3
    last_page = get_last_checkpoint(f"Holman_{data_type}_{code_key}_{code}", default=1)
    total_pages = 2808  # Set this to the actual total number of pages you expect

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future_to_page = {
            executor.submit(fetch_holman_code_batch_data, data_type, code_key, data_key, code, token, page): page
            for page in range(last_page, last_page + total_pages)
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
