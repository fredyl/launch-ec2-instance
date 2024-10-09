limit=100
page=1
all_vechicles=[]
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"

all_vechicles = get_lytx_paging_data(endpoint,limit,page)
print(len(all_vechicles))



def lytx_get_repoonse_from_event_api(endpoint):
    if "videos" in endpoint:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    else:
        base_url = "https://lytx-api.prod5.ph.lytx.com"
    url = base_url + endpoint
    headers = {'x-apikey': api_key }
    response = requests.get(url, headers=headers)
    if response.status_code == 204:
        print(f" Recieved 204, No Content from the API.")
        return response, None
    if response.status_code == 200:
        response_data = response.json()
        return response, response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)
