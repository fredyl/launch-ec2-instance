def replace_null_values(item_list):
    for item in item_list:
        for key, value in item.items():
            if value == "null" or value is None:
                item[key] = ""
    return item_list



global_checkpoint = {}
def save_checkpoint(endpoint_key, page_number):
    global global_checkpoint
    global_checkpoint[endpoint_key] = page_number
    print(f"Checkpoint Updated to {page_number}")


def get_last_checkpoint(endpoint_key, default=0):
    global global_checkpoint
    return global_checkpoint.get(endpoint_key, default)


    

def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=200):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    
    while True:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            pages_fetched = 0
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, stopping pagination")
            if data_list:
                return data_list
            save_checkpoint(endpoint_key, page)
            break

        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
            page += 1
            pages_fetched += 1
            
            if pages_fetched >= batch_size:
                save_checkpoint(endpoint_key, page)
                return data_list    
        else:
            print(f"Error: {response.status_code} {response.text}")
            break

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
   

#iterate through the endpoints to fetch and upsert data
for endpoint_config in holman_coded_endpoints:
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    code =2
    page =1
    # for code in range(1,4):
    while True:
        data_list = fetch_holman_code_batch_data(data_type, code_key, data_key,code,token, batch_size=200)
        clean_data_list = replace_null_values(data_list)
        # print(json.dumps(clean_data_list, indent=4))
        if clean_data_list:
            Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
        else:
            print(f"No data found for {data_type}_{data_key}")
            break
