def paginate_lytx_api(endpoint, limit, page):

    all_vechicles=[]
    while True:
        response,response_data = lytx_get_repoonse_from_event_api(endpoint) #Getting API response and response_data
        print(f"getting data for page:{page}") # print page being processed
        if response_data is None:
            print("No more data to get for endpoint: {endpoint}")
            break

        #Process response if status code 200 and data exists
        if response.status_code == 200 and response_data:
            print(f"Processing page{page}, with {len(response_data)} records")
            all_vechicles.extend(response_data)

            # if 'vehicles' in response_data:
            #     vehicles = response_data['vehicles']
            #     all_vechicles.extend(vehicles)
            # else:
            #     raise Exception(f"No vehicle found for endpoint: {endpoint}")       
            # if not vehicles:
            #     break
        else:
            #If status code is not 200, print status code and break
            print(f"Received status code {response.status_code} for endpoint: {endpoint}")
            break
        page += 1
    print("Pagination completed")
    return all_vechicles

