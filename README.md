def create_dataframe(api_name, group_ids,full_json_data ):

    #creating dataframe data gotten from get_Item_ids_for_all_group_json

    try: # Retrieve JSON data for all groups 
        # full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)

        flattened_data = []
        for group_id, items in full_json_data.items():
            for item in items:
                item['group_id'] = group_id  
                flattened_data.append(item)

        if flattened_data:
            json_strings = [json.dumps(item) for item in flattened_data]      
            data_rdd = spark.sparkContext.parallelize(json_strings)
            data_df = spark.read.json(data_rdd)
            return data_df
        else:
            print("No data available to create a DataFrame.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None
