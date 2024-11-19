def get_holman_api_response(token, endpoint, retries=3):
    #getting an API response from the HOLMAN API endpoint and retry if token expired
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    url = base_url + endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    for attempts in range(retries):
        response = requests.get(url, headers=headers)
        if response.status_code == 204:
            print(f"Received 204, No Content from the API.")
            return  None
        elif response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            print(f"Token expired, refreshing...")
            token = get_token()
            time.sleep(5)
            return get_holman_api_response(token, endpoint, retries=attempts+1)
        else:
            raise Exception("Failed:", response.status_code, response.text)
    raise Exception("Max retries reached for token refresh")



    This wouldn't propagate back to the main token, consider moving this outside of this function, or configure it as a global variable.
