def get_holman_data(token, data_type, data_key,url_ext=None):
    #Define the endpoint and get the response and oing pagination
    
    base_endpoint = f"{data_type}/{delta_url}" if delta_url else data_type
    limit=200
    page=1
    data_list = []

    #Pagination logic
    while True:
        endpoint = f"{base_endpoint}?pageNumber={page}"
        response_data = get_holman_api_response(token, endpoint)
        batch_data = response_data.get(data_key, [])
        if len(batch_data) == 0:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        data_list.extend(batch_data)
        print(f"Processing page{page}, with {len(batch_data)} records")
        page +=1
        
    print(len(data_list))
    
    return data_list
