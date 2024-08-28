def create_groups_dataframe(api_name, groups_data ):
    try: # Retrieve JSON data for all groups 
        # full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)
        # # print("Full JSON data:", json.dumps(full_json_data, indent=2))

        flattened_data = []
        for group_id in groups_data.items():
            for item in items:
                item['group_id'] = group_id  
                flattened_data.append(item)

        if flattened_data:

            json_strings = [json.dumps(item) for item in flattened_data]
           
            data_rdd = spark.sparkContext.parallelize(json_strings)

           
            data_df = spark.read.json(data_rdd)
            # data_df.show()
            return data_df
        else:
            print("No data available to create a DataFrame.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

        groups_data = call_powerbi_api(access_token, 'groups')
# print(json.dumps(groups_data, indent=2))
data_df = create_groups_dataframe("groups", groups_data)
print(data_df)


An error occurred: 'list' object has no attribute 'items'
None
