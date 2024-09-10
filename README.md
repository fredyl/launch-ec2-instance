 if sub_api_endpoint == "refreshSchedule":
        for group_id in group_ids:
            for object_id in object_ids:
                print(len(object_ids))
                url = base_url + f"groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
                # print(url)
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    items = response.json()
                    items['object_id'] = object_id
                    items_llist.append(items)
            return items_llist
    else:
        for group_id in group_ids:
            for object_id in object_ids:
                print(len(object_ids))
                url = base_url + f"/groups/{group_id}/{object_type}/{object_id}/{sub_api_endpoint}"
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    items = response.json().get('value', [])
                    for item in items:
                        item['object_id'] = object_id
                    items_llist.extend(items)
            return items_llist


env = spark.sql("SELECT current_catalog()").collect()[0][0]
