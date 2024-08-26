def convert_json_to_table_structure(full_json_data):
    flat_data = []
    for group_id, items in full_json_data.items():
        for item in items:
            if isinstance(item, dict):
                flattened_item = {"group_id": group_id}
                for key, value in item.items(): 
                    if isinstance(value, dict):
                        flattened_item.update({f"{key}.{k}": v for k, v in value.items()}) 
                    elif isinstance(value, list):
                        flattened_item[key] = str(value)
                    else: 
                        flattened_item[key] = value
                flat_data.append(flattened_item)
            else:
                print(f"Skipping {item} as it is not a dict")            #     flattened_item.update(item)
            #     flat_data.update(flattened_item)
            # else:
            #     print(f"Skipping {item} as it is not a dict")
    
    # print(f"Flat data: {flat_data}")
    return flat_data
    
