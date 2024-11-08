def get_holman_data(token, data_type, data_key,use_delta = False ):
    #Define the endpoint and get the response and oing pagination
    
    limit=200
    page=1
    data_list = []
    table_name = f"bronze.holman_{data_type}"
    use_delta = spark.catalog.tableExists(table_name)
    

    #Pagination logic
    while True:
        endpoint = f"{data_type}?pageNumber={page}"
        response_data = get_holman_data(token, data_type=data_type, data_key=data_key, use_delta=False)
        batch_data = response_data.get(data_key, [])
        if len(batch_data) == 0:
            print(f"No more records on page {page}, Stopping Pagination")
            break
        data_list.extend(batch_data)
        print(f"Processing page{page}, with {len(batch_data)} records")
        page +=1

    print(f"endpoint:{endpoint}")
    print(len(data_list))
    
    return data_list
