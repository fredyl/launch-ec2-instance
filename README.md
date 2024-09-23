def lytx_get_repoonse_from_event_api(endpoint):

    base_url = "https://lytx-api.prod5.ph.lytx.com/video"
    url = base_url + endpoint
    headers = {
        'x-apikey': "t6nu2xUgxpYyA3OUbzpxO6atDQYj1Lxy"  # Add a space between 'Bearer' and the token
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        print(json.dumps(response_data, indent =4))
    else:
        raise Exception("Failed:", response.status_code, response.text)



def statuses_api_response():
    endpoint = "/safety​/events​/statuses"
    
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    if response.status_code == 200:
        response_data = response.json()
        return response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)

# url = "https://lytx-api.prod5.ph.lytx.com/video/safety/events/behaviors"
# output_data = test_get_repoonse_from_event_api(url)
# spark_df = spark.createDataFrame(output_data)

test_get_repoonse_from_event_api()
