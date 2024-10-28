def fetch_Holman_code_data(data_type, code_key, data_key,code):
    data_list = []

    for code in range(1,4):
        print(f"Fetching data from endpoint code={code}")
        page = 1
        total_pages = None
        while True:
            print(f"Fetching page {page} of data from endpoint code={code}")
            endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            if not response_data or data_key not in response_data or not response_data[data_key]:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            elif response.status_code == 200 and response_data:
                batch_data = response_data.get(data_key)
                data_list.extend(batch_data)
                print(f"Processing page{page}, with {len(batch_data)} records")

                if total_pages is None:
                    total_pages = int(response_data.get("totalPages", 1))
                    print(f"Total pages: {total_pages}")
                if page >= total_pages:
                    print("Stopping Pagination")

            else:
                break
                
            page += 1
            pages_fetched += 1
        return data_list



def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=200):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    # total_pages = get_total_pages(data_type, code_key, data_key, code, token)

    while page <= total_pages:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            return data_list  # Return the data list after fetching a batch
        
        print(f"Fetching page {page} of data from {endpoint_key}")
        endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
        response, response_data = get_holman_api_response(token, endpoint)

        if response.status_code == 200:
            batch_data = response_data.get(data_key)

            if batch_data is None or len(batch_data) == 0:
                print(f"No data found for {data_type}_{data_key} on page {page}, stopping pagination")
                save_checkpoint(endpoint_key, page)
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
