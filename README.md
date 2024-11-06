def replace_null_values_df(data_key):
    if isinstance (data_key, dict):
        return {k: replace_null_values_df(v) if v is not None else " " for k, v in data_key.items()}
    elif isinstance (data_key, list):
        return [replace_null_values_df(item) if item is not None else " " for item in data_key]
    else:
        return data_key if data_key is not None else " "



def generate_paginated_urls(data_type,data_key, code_key, code_value,total_pages,batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    num_batches = (total_pages // batch_size) + 1
    print(f" num_batches : {num_batches}")
    for i in range(num_batches):
        for page in range(i * batch_size + 1, min((i + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
            # print(pagination_urls)
    print('len', len(pagination_urls))
    return pagination_urls, num_batches


holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    "maintenance": ("maintenance", "clientVehicleNumber"),
    "accidents": ("accident", "record_id"),
    "odometer": ("odometerHistory", "vin"),
    "persons": ("person", "contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    data_list = replace_null_values(data_list)
    # print(json.dumps(data_list, indent=4))
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
print("All data fetched")

