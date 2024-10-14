def get_holman_api_response(token, endpoint):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json'
    }
    headers['Authorization'] = f"Bearer {token}"
    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        print(f" Recieved 204, No Content from the API.")
        return response, None
    if response.status_code == 200:
        response_data = response.json()
        return response, response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)

def fetch_Holman_code_data(data_type, code_key, data_key):
    data_list = []
    for code in range(1,3):
        print(f"Fetching data from endpoint code={code}")
        page = 1
        while True:
            print(f"Fetching page {page} of data from endpoint code={code}")
            endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
            response, response_data = get_holman_api_response(token, endpoint)
            if data_key not in response_data or not response_data[data_key]:
                print(f"No more records on page {page}, Stopping Pagination")
                break
            data_list.extend(response_data[data_key])
            if int(response_data['pageNumber']) >= int(response_data['totalPages']):
                break
            page +=1
    return data_list

fetch_violation_data = fetch_Holman_code_data(data_type="violation", code_key="violationDateCode", data_key="violations")
fetch_billing_data = fetch_Holman_code_data(data_type="billing", code_key="billingTypeCode", data_key="billing")
fetch_fuels_data = fetch_Holman_code_data(data_type="fuels", code_key="transDateCode", data_key="us")
