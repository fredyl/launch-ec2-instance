 for group_id in group_data_id:
        for object_id in object_ids:
            url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                if sub_api_endpoint == "refreshSchedule":#sub_api_endpoint is refreshSchedule the process since the response is a json object
                    items = response.json()
                    items[key_id] = object_id
                    items_llist.append(items)
                else:
                    items = response.json().get('value', [])
                    for item in items:
                        item[key_id] = object_id
                    items_llist.extend(items)
    return items_llist
    
