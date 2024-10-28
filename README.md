def fetch_holman_code_batch_data(data_type, code_key, data_key, code, token, batch_size=200):
    data_list = []
    endpoint_key = f"Holman_{data_type}_{code_key}_{code}"
    last_page = get_last_checkpoint(endpoint_key, default=1)
    page = last_page
    pages_fetched = 0
    total_pages = None

    while True:
        if pages_fetched >= batch_size:
            # Save checkpoint
            save_checkpoint(endpoint_key, page)
            print(f"Fetched batch size {batch_size} from {endpoint_key}, saving checkpoint")
            pages_fetched = 0  # Return the data list after fetching a batch
        
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

            if total_pages is None:
                total_pages = int(response_data.get("totalPages", 1))
                print(f"Total pages: {total_pages}")
            if page >= total_pages:
                print("Stopping Pagination")
                break

            page += 1
            pages_fetched += 1
        else:
            print(f"Error: {response.status_code} {response.text}")
            break

    # Save checkpoint if loop completes
    save_checkpoint(endpoint_key, page)
    return data_list



def process_endpoint(endpoint_config, token):
    data_type = endpoint_config["data_type"]
    code_key = endpoint_config["code_key"]
    data_key = endpoint_config["data_key"]
    primary_key = endpoint_config["primary_key"]
    # code =2
    page =1
    for code in range(1,4):
    # while True:
        data_list = fetch_holman_code_batch_data(data_type, code_key, data_key,code, token, batch_size=200)
        if data_list is not None:
            clean_data_list = replace_null_values(data_list)
        # print(json.dumps(clean_data_list, indent=4))
            if clean_data_list:
                Holman_Upsert_data(data_type, data_key, clean_data_list, primary_key)
        else:
            print(f"No data found for {data_type}_{data_key}")
            break


def process_holman_endpoint_concurrently(endpoint,max_workers=4, batch_size =200):
    for endpoint_config in holman_coded_endpoints:
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for code in range(1, 4):
                futures = [
                    executor.submit(fetch_holman_code_batch_data, endpoint_config["data_type"], endpoint_config["code_key"], endpoint_config["data_key"], code, token, batch_size)
                    ]
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as exc:
                        print(f"Exception: {exc}")

