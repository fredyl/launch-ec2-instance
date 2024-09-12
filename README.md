def get_all_groups_ids(access_token):
    try:
        # Fetch group IDs from the Power BI API
        groups_data = call_powerbi_api(access_token, 'groups')[1]
        group_ids = [group['id'] for group in groups_data['value']]
        return group_ids
    except Exception as e:
        print(f"Error fetching group IDs: {e}")
        return []

def get_all_object_type_items_for_groups(access_token, object_type, key_id, sub_api_endpoint):
    group_ids = get_all_groups_ids(access_token)  # Fetch group IDs
    items_list = []

    # Combine group and object data retrieval without looping through group_ids
    for group_id in group_ids:
        try:
            # Fetch the object type items for each group ID in one go (without looping)
            endpoint = f"/groups/{group_id}/{object_type}/{sub_api_endpoint}"
            response, data = call_powerbi_api(access_token, endpoint)
            
            if response.status_code == 200:
                # If sub_api_endpoint is refreshSchedule, add object IDs to data
                if sub_api_endpoint == "refreshSchedule":
                    data[key_id] = group_id
                    items_list.append(data)
                else:
                    items = data.get('value', [])
                    for item in items:
                        item[key_id] = group_id
                    items_list.extend(items)

        except Exception as e:
            print(f"Error for group_id {group_id}: {e}")

    return items_list

# Example usage
access_token = "your_access_token_here"
object_type = "datasets"
key_id = "id"
sub_api_endpoint = "refreshSchedule"

# Fetch all group IDs and object type items in one go
items = get_all_object_type_items_for_groups(access_token, object_type, key_id, sub_api_endpoint)
