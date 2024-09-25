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

[CANNOT_INFER_TYPE_FOR_FIELD] Unable to infer the type of the field `devices`.
PySparkTypeError: [CANNOT_MERGE_TYPE] Can not merge type `StringType` and `ArrayType`.

During handling of the above exception, another exception occurred:
PySparkTypeError                          Traceback (most recent call last)
