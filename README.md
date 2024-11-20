holman_endpoints = {
    "vehicles": ("inventory", "clientVehicleNumber"),
    # "accidents": ("accident", "clientVehicleNumber"),
    # "odometer": ("odometerHistory", "vin"),
    # "persons": ("person", "contactEmployeeId")
}

if "vehicles" in holman_endpoints:
    schema = StructType([
    StructField("orderReceivedDate",StringType(),True),
    StructField("orderPlacedDate",StringType(),True),
    StructField("scheduledProductionDate",StringType(),True),
    StructField("deliveryPaperMailed",StringType(),True),
    StructField("shipDate",StringType(),True),
    StructField("deliveredToDealerDate",StringType(),True),
    StructField("notifiedOfDelivery",StringType(),True),
    StructField("upfitInvoiceDate",StringType(),True)

])

#iterate through the endpoints to fetch and upsert data
for data_type, (data_key, primary_key) in holman_endpoints.items():
    upd_endpoints = update_endpoints(data_type)
    print(f"Processing {data_type}")

    if upd_endpoints and upd_endpoints.get("run_all"):
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key)
    elif upd_endpoints and "delta_url" in upd_endpoints:
        delta_url = upd_endpoints["delta_url"]
        # endpoint = f"{data_type}/{delta_url}"
        data_list = get_holman_data(token, data_type=data_type, data_key=data_key, url_ext=delta_url)
    else:
        print(f"Skipping {data_type} endpoint is not defined")
        continue
    # print(json.dumps(data_list, indent=4))
    
    # data_list = replace_null_values(data_list)
    if data_list:
        Holman_Upsert_data(data_type, data_list, primary_key, merge_keys=None, schema=schema)
    else:
        print(f"No data found for {data_type}")
print("All data fetched")
