def call_powerbi_api(access_token, endpoint, params=None):
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
    base_url = 'https://api.powerbi.com/v1.0/myorg/'
    url = base_url + endpoint
    response = requests.get(url, headers=headers, params=params)

    for _ in range(3):
        if response.status_code == 200:
            data = response.json()
            return data['value'] if 'value' in data else data, response.status_code
        
        elif response.status_code == 429:
            retry_after = int(response.headers.get('Retry-After', 8))
            time.sleep(retry_after)
        
        else:
            raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")
