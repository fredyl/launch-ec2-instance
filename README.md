def get_holman_data(token, data_type, data_key,url_ext=None):
    #Define the endpoint and get the response and oing pagination
    
    base_endpoint = f"{data_type}/{delta_url}" if delta_url else data_type
    limit=200
    page=1
    data_list = []

    #Pagination logic
    while True:
        endpoint = f"{base_endpoint}?pageNumber={page}"
        response_data = get_holman_api_response(token, endpoint)
        batch_data = response_data.get(data_key, [])
        if len(batch_data) == 0:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        data_list.extend(batch_data)
        print(f"Processing page{page}, with {len(batch_data)} records")
        page +=1
        
    print(len(data_list))
    
    return data_list


#define a set of endpoints with corresponding data_type and data_keys for Holman API without code_keys
holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    "maintenance": ("maintenance", "clientVehicleNumber"),
    "accidents": ("accident", "clientVehicleNumber"),
    "odometer": ("odometerHistory", "vin"),
    "persons": ("person", "contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    upd_endpoints = update_endpoints(data_type)
    print(f"Processing {data_type}")

    if upd_endpoints and upd_endpoints.get("run_all"):
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
    elif upd_endpoints and "delta_url" in upd_endpoints:
        delta_url = upd_endpoints["delta_url"]
        endpoint = f"{data_type}/{delta_url}"
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
    else:
        print(f"Skipping {data_type} endpoint is not defined")
        continue
    
    data_list = replace_null_values(data_list)

    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}")
print("All data fetched")
