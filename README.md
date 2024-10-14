def get_lytx_paging_data(endpoint, page):
    all_vehicles = []
    limit = 50
    page = 1
    while True:
        if endpoint == "/vehicles/all?limit={limit}&page={page}&includeSubgroups=true":
            page_endpoint = f"{endpoint}"
            print(page_endpoint)
        else:
            endpoint == f"/video/vehicles?PageNumber={page}&PageSize={limit}"
            page_endpoint = f"{endpoint}"
            print(page_endpoint)

        response,response_data = lytx_get_repoonse_from_event_api(page_endpoint)
        status_code = response.status_code
        if response_data is None or status_code ==204:
            print(f" No Content on page {page}, Stopping Pagination")
            break
        if status_code == 200 and 'vehicles' in response_data:
            vehicles = response_data['vehicles']
            print(f"Processing page{page}, with {len(vehicles)} records")
            if not vehicles:
                break
            all_vechicles.extend(vehicles) 
        else:
            break
        page +=1 #move to next page
    print("Pagination completed")
    return all_vechicles
