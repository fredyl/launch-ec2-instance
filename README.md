def get_lytx_paging_data(endpoint, limit, page):
    all_vechicles = []
    while True:
        page_endpoint = f"{endpoint}pages={page}*"
        response,response_data = lytx_get_repoonse_from_event_api(endpoint)
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
        page +=1
    print("Pagination completed")
    return all_vechicles


limit=100
page=1
all_vechicles=[]
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/vehicles/all?pages={page}&includeSubgroups=true"

all_vechicles = get_lytx_paging_data(endpoint,page)
print(len(all_vechicles))


Exception: ('Failed:', 400, '{"errors":{"includeSubgroups":["The value is not valid for IncludeSubgroups."]},"type":null,"title":"One or more validation errors occurred.","status":400,"detail":null,"instance":null,"extensions":{}}')
File <command-310280906947074>, line 7
      4 table_name = f"bronze.lytx_video_vehicles_vehicleId"
      5 endpoint = f"/vehicles/all?pages={page}&includeSubgroups=true"
----> 7 all_vechicles = get_lytx_paging_data(endpoint,page)
      8 print(len(all_vechicles))
File <command-1002329549987713>, line 16, in lytx_get_repoonse_from_event_api(endpoint)
     14     return response, response_data
     15 else:
---> 16     raise Exception("Failed:", response.status_code, response.text)
