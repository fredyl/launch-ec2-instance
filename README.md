def get_holman_data(toke, endpoint, data_key, ):
    limit=200
    page=1
    data_list = []

    while True:
        paginated_endpoint = f"{endpoint}?pageNumber={page}"
        response, response_data = get_holman_api_response(token, paginated_endpoint)
        if response.status_code == 200 and response_data:
            batch_data = response_data.get(data_key, [])
            if len(batch_data) == 0:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            data_list.extend(batch_data)
            print(f"Processing page{page}, with {len(batch_data)} records")
            page +=1
        else:
            break
    print(f"endpoint:{paginated_endpoint}")
    print(len(data_list))
    
    return data_list

accident_data = get_holman_data(token, endpoint ="accidents", data_key="accident")
odometer_data = get_holman_data(token, endpoint ="Odometer", data_key="odometerHistory")
order_data = get_holman_data(token, endpoint ="orders", data_key="order")
persons_data = get_holman_data(token, endpoint ="persons", data_key="person")
