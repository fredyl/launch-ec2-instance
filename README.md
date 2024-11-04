 data_list = []
        for row in cleaned_data.collect():
            if data_key in row and isinstance(row[data_key], str) and not row[data_key].startswith("Error") and not row[data_key].startswith("Exception"):
                try:
                    parsed_data = json.loads(row[data_key])
                    # if isinstance(parsed_data, list):
                    data_list.extend(parsed_data)
                    # else:
                    #     data_list.append(parsed_data)
                    
                    print(json.dumps(data_list,indent=2))
                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON for URL: {row.url}: {e}")

        # Upsert data if available
        if data_list:
            # cleaned_data = replace_null_values(data_list)
            Holman_Upsert_data(data_type, data_key, primary_key)
            print(f"Data upserted for data type {data_type} with code value {code_value}")
    for f in dbutils.fs.ls(chk_path):
        dbutils.fs.rm(f.chk_path, True)
