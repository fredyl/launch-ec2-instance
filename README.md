for _ in iterator:
        while pages_fetched < batch_size:
            print(f"Fetching page {page} of data from {endpoint_key}")
            endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            print(f"response for {endpoint}: {response.status_code}")
            
            if response.status_code != 200 or not response_data.get(data_key):
                print(f"No more records on page {page}, stopping pagination")
                print(data_key)
                break

            fetched_data = response_data[data_key]
            
            if int(response_data.get('pageNumber')) >= total_pages:
                break

            if not fetched_data:
                break

            data_list.extend(fetched_data)
            print(f"Processing page {page}, with {len(fetched_data)} records")

            page += 1
            pages_fetched += 1

            save_checkpoint(endpoint_key, page)

        if pages_fetched >= batch_size:
            print(f"Fetched batch size {batch_size}, stopping for batch")
            break

    print(f"Finished processing partition with {len(data_list)} records")
    return iter(data_list)
