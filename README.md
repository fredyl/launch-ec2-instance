def convert_holman_data(result_df, data_key):
    """
    Convert paginated API response data to a consolidated list.
    Args:
        result_df (DataFrame): DataFrame containing API response JSON paths.
        data_key (str): Key to extract data from JSON objects.

    Returns:
        list: Consolidated list of data extracted from JSON.
    """
    # Collect file paths from the result DataFrame
    file_paths = [row.data for row in result_df.select("data").collect()]
    if not file_paths:
        print("No file paths found. Skipping conversion.")
        return []

    # Read the JSON files into a single DataFrame
    o_df = spark.read.json(file_paths)
    if o_df.isEmpty():
        print("No data found in the JSON files. Skipping conversion.")
        return []

    # Extract and flatten the data
    try:
        if data_key in o_df.columns:
            extracted_data = o_df.select(data_key).rdd.flatMap(lambda x: x[0] if isinstance(x[0], list) else []).collect()
            print(f"Total records accumulated: {len(extracted_data)}")
            return extracted_data
        else:
            print(f"Data key '{data_key}' not found in JSON schema.")
            return []
    except Exception as e:
        print(f"Error processing JSON data: {str(e)}")
        return []
