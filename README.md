if response_data is None:
        print(f" recieved 204, No Content on page {page}, Stopping Pagination")
        break
    if status_code == 200 and response_data:
        print(f"Processing page{page}, with {len(response_data)} records")
        all_events_metadata.extend(response_data)
    else:
        break
    page +=1


def paginate_lytx_api(endpoint, limit, page):

    all_vechicles=[]
    while True:
    response,response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    if 'vehicles' in response_data:
        vehicles = response_data['vehicles']
        all_vechicles.extend(vehicles)
        if len(vehicles) < limit:
            break
    else:
        raise Exception(f"No vehicles key found in response data for endpoint: {endpoint}") 
    page += 1
