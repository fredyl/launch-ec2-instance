def get_object_type_items(access_token, object_type, object_id, sub_api_endpoint):
    """
    Retrieves items from object_type (e.g. datasets, reports, dataflows) based on the object_id.
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_object_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []
    
      for group_id, object_ids in group_id_and_object_id_dict.items():
        print(f"Processing group: {group_id}")
        for obj_id in object_ids:
            endpoint = f"groups/{group_id}/{object_type}/{obj_id}/{sub_api_endpoint}"
            try:
                # Call the API and append the response
                response, json_data = call_powerbi_api(access_token, endpoint)
                all_data.append(json_data)
            except HTTPError as e:
                # Handle 403 errors gracefully
                if e.response.status_code == 403:
                    print(f"Unauthorized access for group {group_id}. Skipping...")
                else:
                    raise  # Reraise the error for other types of HTTP errors

    return all_data

sub_api_endpoint):
    """
    retrieves items from object_type (e.g. datasets, reports, dataflows) based on the
    object_id's
    """
    groups_ids = get_all_groups_ids(access_token)
    group_id_and_ojbect_id_dict = get_item_ids_for_all_groups(access_token, groups_ids, object_type, object_id)
    all_data = []

    for group_ids, object_ids in group_id_and_ojbect_id_dict.items():
        for obj_id in object_ids:
            endpoint= f"groups/{group_ids}/{object_type}/{obj_id}/{sub_api_endpoint}"
                # print(endpoint)
            response = call_powerbi_api(access_token, endpoint)[1]
            if response == 403:
                    print(f"Skipping item user does not have access to {group_ids}")
                    continue
            else:
                if response == 200:
                    response = call_powerbi_api(access_token, endpoint)[0]
                    all_data.append(response)
                    # print(response)
                
    return all_data

def call_powerbi_api(access_token, endpoint, params=None):

    url = 'https://api.powerbi.com/v1.0/myorg/'
    url = url + endpoint

        # print(f"calling url")
    headers ={ 'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
        }
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
            # print(json.dumps(data, indent=2))
        return data['value'] if 'value' in data else data, response.status_code
    
    elif response.status_code == 429:
        retry_after = int(response.headers.get('Retry-After', 8))
            # print("Rate limit exceeded, retrying after 8 seconds")
        time.sleep(retry_after)
    
    else:
        raise Exception(f"Request failed with status {response.status_code},Response: {response.text}")

call_powerbi_api(access_token, 'groups')[1]
