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

fetch_Holman_code_data(data_type="fuels", code_key="transDateCode", data_key="us", code = range(1,4))
