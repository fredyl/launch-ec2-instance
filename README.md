def get_lytx_paging_data(endpoint, page):
    all_vehicles = []
    limit = 100
    page = 1
    while True:
        if endpoint =="/vehicles/all":
            page_endpoint = f"{endpoint}?pages={page}"
        else:
            page_endpoint = f"{endpoint}?PageNumber={page}&PageSize={limit}"
               
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


    limit = 50
all_vehicles=[]
page =1
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"

all_vehicles = get_lytx_paging_data(endpoint, page)


Exception: ('Failed:', 400, '{"errors":{"pageSize":["The value is not valid for PageSize."]},"title":"One or more validation errors occurred.","status":400,"extensions":{}}'
