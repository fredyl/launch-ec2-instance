 if isinstance(json_object, list) and len(json_object) > 0 and isinstance(json_object[0], dict):
        json_object = json_object[0]  # Extract the first dictionary from the list
    
    # Check if json_object is a dictionary and contains the 'value' key
    if isinstance(json_object, dict) and 'value' in json_object:
        data_list = json_object['value']
    else:
        # Print the JSON object to debug
        print("Unexpected JSON format:")
        print(json.dumps(json_object, indent=2))
        return
