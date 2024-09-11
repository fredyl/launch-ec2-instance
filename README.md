import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_object_items(group_id, object_id, object_type, sub_api_endpoint, headers):
    url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        if sub_api_endpoint == "refreshSchedule":
            items = response.json()
            items[key_id] = object_id
            return items
        else:
            items = response.json().get('value', [])
            for item in items:
                item[key_id] = object_id
            return items
    return []

def get_object_type_items(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)[0]
    object_ids = get_all_object_id_for_each_object_type(access_token, object_type, key_id)[1]

    items_llist = []

    with ThreadPoolExecutor() as executor:
        futures = []
        for group_id in group_ids:
            for object_id in object_ids:
                futures.append(executor.submit(fetch_object_items, group_id, object_id, object_type, sub_api_endpoint, headers))

        for future in as_completed(futures):
            items = future.result()
            if isinstance(items, list):
                items_llist.extend(items)
            elif isinstance(items, dict):
                items_llist.append(items)

    return items_llist
