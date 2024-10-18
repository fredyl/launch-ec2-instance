Exception: ('Failed:', 400, '{"type":"https://tools.ietf.org/html/rfc7231#section-6.5.1","title":"One or more validation errors occurred.","status":400,"traceId":"80003f12-0000-cf00-b63f-84710c7967bb","errors":{"pageNumber":["The value \'{}\' is not valid."]}}')
File <command-3094085711592474>, line 39
     37 # for code in range(1,4):
     38 while True:
---> 39     data_list = fetch_holman_code_batch_data(data_type, code_key, data_key,code,batch_size=50)
     40     clean_data_list = replace_null_values(data_list)
     41     # print(json.dumps(clean_data_list, indent=4))
File <command-310280906944989>, line 16, in get_holman_api_response(token, endpoint)
     14     return response, response_data
     15 else:
---> 16     raise Exception("Failed:", response.status_code, response.text)



def fetch_holman_code_batch_data(data_type, code_key, data_key, code, batch_size=50):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    
    while True:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            pages_fetched = 0
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        
        if not response_data or data_key not in response_data or not response_data[data_key]:
            print(f"No more records on page {page}, stopping pagination")
            if data_list:
                return data_list
            save_checkpoint(endpoint_key, page)
            break

        if response.status_code == 200:
            fetched_data = response_data[data_key]
            data_list.extend(fetched_data)
            page += 1
            pages_fetched += 1
            
            if pages_fetched >= batch_size:
                save_checkpoint(endpoint_key, page)
                return data_list    
        else:
            print(f"Error: {response.status_code} {response.text}")
            break
