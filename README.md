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
