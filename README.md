def call_powerbi_api(access_token, endpoint, params=None):
    headers= { 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
    base_url = 'https://api.powerbi.com/v1.0/myorg/'
    url = base_url + endpoint
    for _ in range(3):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            json_data = response.json()
            return response, json_data
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after)
        else:
            error_message =(
                f"Request failed with status {response.status_code}",
                f"Response: {response.text}, URL: {url}",
            )
            raise Exception(error_message)
