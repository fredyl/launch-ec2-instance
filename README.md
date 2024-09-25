![pic1](https://github.com/user-attachments/assets/4758ce09-9ef6-46f8-ac09-f6ffbac6e9c5)
from pyspark.sql import Row
def statuses_api_response(parent_key='', sep= '_'):
    endpoint = "/video/vehicles"   

    response_data = lytx_get_repoonse_from_event_api(endpoint)

    if 'vehicles' not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
    vehicle_ids= [vehicle['id'] for vehicle in response_data['vehicles']]

    struct_response_data =[]
    all_keys = set()
    for vehicle_id in vehicle_ids:
        endpoint = f"/video/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        vehicle_json_data ={}
        for key, value in vehicle_data.items():
            if isinstance(value, dict) or isinstance(value, list):
                vehicle_json_data[key] =json.dumps(value)
            else:
                vehicle_json_data[key] =value
        all_keys.update(vehicle_json_data.keys())
        # print(json.dumps(response_data,indent=2))

    struct_response_data.append(vehicle_json_data)
    for data in struct_response_data:
        for key in all_keys:
            if key not in data:
                data[key] = None
    
    rows = [Row(**data) for data in struct_response_data]
    if isinstance(struct_response_data, list):
        df = spark.createDataFrame(struct_response_data)


current_time = current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)

    # Ensure `devices` is treated as a JSON string
    df = df.withColumn("devices", col("devices").cast(StringType()))

    # Attempt to parse the devices JSON column dynamically
    try:
        df = df.withColumn("devices_parsed", from_json(col("devices"), "array<struct<*>>"))
    except Exception as e:
        print(f"Error parsing devices: {e}")
        # Fallback if parsing fails
        df = df.withColumn("devices_parsed", col("devices"))

    # Explode the `devices_parsed` array (if it contains multiple items)
    df = df.withColumn("device", explode(col("devices_parsed")))

    # Dynamically select columns (exploded fields + existing fields)
    df = df.select("*", "device.*").drop("devices", "device", "devices_parsed")

    # Display the DataFrame
    df.display()
