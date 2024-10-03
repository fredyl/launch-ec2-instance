https://lytx-api.prod5.ph.lytx.com/video/safety/eventsWithMetadata?limit=1000&from=2022-01-01T00:00:00.00Z&to=2022-01-06T00:00:00.00Z&dateOption=lastUpdatedDate&sortBy=lastUpdatedDate&page=3



def lytx_get_repoonse_from_event_api(endpoint):
    if "videos" in endpoint:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    else:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    url = base_url + endpoint
    headers = {'x-apikey': api_key }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response, response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)
