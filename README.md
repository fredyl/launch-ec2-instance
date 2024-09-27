is there a way to store all the group_ids the will have to just call each group id from where its been stored than etirating through each and every one of them


def vehicles_vehicleId_api_response():
    endpoint = "/vehicles/all"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    if 'vehicles' not in response_data:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}")
    group_ids = [vehicle['groupId'] for vehicle in response_data['vehicles'] if 'groupId' in vehicle]
    
    vehicle_ids = []
    for group_id in group_ids:
        endpoint = f"/vehicles/all?groupid={group_id}"
        group_vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
        
        if 'vehicles' in group_vehicle_data:
            group_vehicle_ids = [vehicle['id'] for vehicle in group_vehicle_data['vehicles'] if 'id' in vehicle]
            vehicle_ids.extend(group_vehicle_ids)    
            for vehicle_id in group_vehicle_ids:
                endpoint = f"/vehicles/{vehicle_id}"
                vehicle_data = lytx_get_repoonse_from_event_api(endpoint)
                # Assuming you want to do something with vehicle_data here
    return vehicle_ids
