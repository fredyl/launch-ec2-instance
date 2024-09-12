import requests
import concurrent.futures

def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    """
    Retrieves items from object_type (e.g. datasets, reports, dataflows) based on the object_id's.
    Optimized to handle API requests concurrently.
    """
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_list = []

    # Define a helper function to make the API call
    def fetch_items(group_id, object_id):
        url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if sub_api_endpoint == "refreshSchedule":
                data[key_id] = object_id
                return [data]
            else:
                items = data.get('value', [])
                for item in items:
                    item[key_id] = object_id
                return items
        return []

    # Use ThreadPoolExecutor for concurrent execution of requests
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for group_id in group_ids:
            for object_id in object_ids:
                futures.append(executor.submit(fetch_items, group_id, object_id))
        
        # Gather results from all the futures
        for future in concurrent.futures.as_completed(futures):
            items_list.extend(future.result())

    return items_list