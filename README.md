def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, total_pages, batch_size=100):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    
    while page <= total_pages:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            return data_list  # Return the data list after fetching a batch
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)
        
        if not response_data.get(data_key):
            print(f"No more records on page {page}, stopping pagination")
            if data_list:
                save_checkpoint(endpoint_key, page)
        break

        if response.status_code == 200:
            batch_data = response_data[data_key]
            if not batch_data:
                print(f"No data found for {data_type}_{data_key} on page {page}, stopping pagination")
                break
            print(f"Processing page {page}, with {len(batch_data)} records")
            data_list.extend(batch_data)
            page += 1
            pages_fetched += 1
        else:
            print(f"Error: {response.status_code} {response.text}")
            break
    
    # Save checkpoint if loop completes
    save_checkpoint(endpoint_key, page)
    return data_list
