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
    data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    data_list = replace_null_values(data_list)

    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
print("All data fetched")
