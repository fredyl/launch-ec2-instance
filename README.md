if json_object:
        if isinstance(json_object, dict) and 'value' in json_object:
            # Extract 'value' from the JSON object
            data_list = json_object['value']
        else:
            # Fallback if 'value' is not present
            data_list = json_object
