An error occurred: 'str' object does not support item assignment


def groups_data_df(access_token):
    try: # Retrieve JSON data for all groups 
        groups_data = get_groups_id(access_token)
        print("Full JSON data:", json.dumps(groups_data, indent=2))

        flattened_data = []
        for group_id, items in groups_data.items():
            for item in items:
                item['group_id'] = group_id  
                flattened_data.append(item)

        if flattened_data:
            json_strings = [json.dumps(item) for item in flattened_data]
            data_rdd = spark.sparkContext.parallelize(json_strings)
            data_df = spark.read.json(data_rdd)
            display(data_df)
        # return data_df
        else:
            print("No data available to create a DataFrame.")
            # return None

    except Exception as e:
            print(f"An error occurred: {str(e)}")
            # return None
