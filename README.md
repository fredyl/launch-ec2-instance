def get_vehicle_data(endpoint_template, limit, include_subgroups=False):
    page = 1
    all_vehicles = []

    while True:
        # Modify endpoint based on whether to include subgroups or not
        if include_subgroups:
            endpoint = f"{endpoint_template}?limit={limit}&page={page}&includeSubgroups=true"
        else:
            endpoint = f"{endpoint_template}?PageNumber={page}&PageSize={limit}"
        
        # Make the API call
        response, response_data = lytx_get_repoonse_from_event_api(endpoint)
        print(f"Getting data for page: {page}")

        # Handle the case where the response is empty or status code is 204 (No Content)
        status_code = response.status_code
        if response_data is None or status_code == 204:
            print(f"No content on page {page}, stopping pagination.")
            break

        # Check if 'vehicles' key is in response data
        if 'vehicles' in response_data:
            vehicles = response_data['vehicles']
            all_vehicles.extend(vehicles)

            # Stop if the number of vehicles is less than the limit or data is empty
            if len(vehicles) < limit:
                break
        else:
            # Raise exception if 'vehicles' key is not found
            raise Exception(f"No 'vehicles' key found in response data for endpoint: {endpoint}")

        # Increment page number for next iteration
        page += 1

    print(f"Total vehicles retrieved: {len(all_vehicles)}")
    return all_vehicles
