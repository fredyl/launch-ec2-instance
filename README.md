use the below code iterate through group_id and get the vehicle and return the data off all the vehicles ids group from all the group id

def vehicles_vehicleId_api_response():
    endpoint = "/vehicles/all"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    if 'vehicles' not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
    group_ids = [vehicle['groupId'] for vehicle in response_data['vehicles'] if 'groupId' in vehicle]

    response_data = []
    for group_id in group_ids:
        endpoint = f"/vehicles/all?groupid={group_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        vehicle_ids = [vehicle['id'] for vehicle in vehicle_data['vehicles']]
        response_data.extend(vehicle_ids)
    return vehicle_ids
            

    response_data =[]
    for vehicle_id in vehicle_ids:
        endpoint = f"/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        response_data.append(vehicle_data)   
    return response_data
