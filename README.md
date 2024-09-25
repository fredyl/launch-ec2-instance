def statuses_api_response(parent_key='', sep= '_'):
    endpoint = "/video/vehicles"   

    response_data = lytx_get_repoonse_from_event_api(endpoint)

    if 'vehicles' not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
    vehicle_ids= [vehicle['id'] for vehicle in response_data['vehicles']]

    response_data =[]
    for vehicle_id in vehicle_ids:
        endpoint = f"/video/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        response_data.append(vehicle_data)
        # print(json.dumps(response_data,indent=2))

    flatten_data =[]
    for vehicle_data in response_data:
        flatten_data.append(flatten_json(vehicle_data, parent_key, sep))
    return  flatten_data
    # rdd = spark.sparkContext.parallelize([response_data])
    # df = spark.read.json(rdd)
    # if isinstance(response_data, list):
    #     df = spark.createDataFrame(response_data)
    # current_time = F.current_timestamp()
    # df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
    # df.display()
