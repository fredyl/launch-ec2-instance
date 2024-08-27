def create_dataframe_from_json(api_name, group_ids):
    try:
        # Step 1: Retrieve JSON data for all groups
        full_json_data = get_Item_ids_for_all_group_json(access_token, group_ids, api_name)

        # Step 2: Flatten the JSON data
        flattened_data = []
        for group_id, items in full_json_data.items():
            for item in items:
                item['group_id'] = group_id  # Add group_id to each item
                flattened_data.append(item)

        if flattened_data:
            # Step 3: Create an RDD from the flattened data
            data_rdd = spark.sparkContext.parallelize(flattened_data)

            # Step 4: Convert RDD to DataFrame
            data_df = spark.read.json(data_rdd)
            data_df.show()
            return data_df
        else:
            print("No data available to create a DataFrame.")
            return None

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

# Example usage
api_name = "datasets"  # or "dataflows" depending on what you need
dataset_df = create_dataframe_from_json(api_name, group_ids)
