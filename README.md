    url = 'https://api.powerbi.com/v1.0/myorg/'
    url = url + endpoint
    headers ={ 'Authorization': f'Bearer {access_token}',
              'Content-Type': 'application/json'
    }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
       
        return data['value'] if 'value' in data else data
    # elif response.status_code == 429:
    #     retry_after = int(response.headers.get('Retry-After', 8))
    #     time.sleep(retry_after)
    else:
        raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")

