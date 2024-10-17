def fetch_Holman_code_data(data_type, code_key, data_key, code, page_size=100):
    token = "your_api_token_here"
    base_url = "https://api.example.com/data"
    last_page = get_last_checkpoint()
    all_data = []
    more_data = True
    current_page = last_page + 1

    while more_data:
        endpoint = f"{base_url}/{data_type}?{code_key}={code}&page={current_page}&pageSize={page_size}"
        response, response_data = get_holman_api_response(token, endpoint)
        if response.status_code == 200:
            data = response_data.get(data_key, [])
            all_data.extend(data)
            more_data = len(data) == page_size
            current_page += 1
        else:
            more_data = False
            print(f"Error fetching data: {response.status_code}")

    save_checkpoint(current_page - 1)
    return all_data
