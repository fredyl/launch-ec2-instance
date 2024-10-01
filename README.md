limit = 100
page = 1
all_vehicle_data=[]
complex_columns = ['devices']

while True:
    endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    # print(json.dumps(response_data, indent=2))
    print(f"getting data for page:{page}")
    if "vehicles" not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")

    vehicles = response_data['vehicles']
    all_vehicle_data.append(vehicles)
    # print(json.dumps(all_vehicle_data, indent=2))

    if len(vehicles) < limit:
        break
    page += 1

    # print(json.dumps(all_vehicles, indent=2))
   

# if all_vehicle_data:
# data_dict =json.dumps(all_vehicle_data)
if all_vehicle_data:
    df = spark.createDataFrame(all_vehicle_data)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    df.display()
    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")


        [CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field `_1`.
