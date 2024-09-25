def fetch_vehicle_details():
    # Step 1: Fetch all vehicles and extract IDs
    all_vehicles_endpoint = "/vehicles/all"
    response_data = lytx_get_repoonse_from_event_api(all_vehicles_endpoint)
    
    # Extract vehicle IDs from the response
    if 'vehicles' not in response_data:
        raise Exception("No 'vehicles' key found in the response data")
    
    vehicle_ids = [vehicle['id'] for vehicle in response_data['vehicles']]  # List of vehicle IDs
    
    # Step 2: Fetch details for each vehicle by ID
    vehicle_details = []
    for vehicle_id in vehicle_ids:
        vehicle_endpoint = f"/vehicles/{vehicle_id}"
        vehicle_data = lytx_get_repoonse_from_event_api(vehicle_endpoint)  # Fetch details by ID
        vehicle_details.append(vehicle_data)  # Store the vehicle details
    
    return vehicle_details  # Return the list of all vehicle details
