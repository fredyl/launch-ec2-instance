while True:
    endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")

    #check if vehicle key word is in response data
    if 'vehicles' in response_data:
        vehicles = response_data['vehicles']
        all_vechicles.extend(vehicles)

        #check if the length of the vehicles list is less than the limit
        if len(vehicles) < limit:
            break
    else:#Raise exception if no vehicles key is found
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1 #increment the page number by 1
return all_vechicles
print(len(all_vechicles))
