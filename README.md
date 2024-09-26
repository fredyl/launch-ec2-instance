how do I modify this to include events and status in the table name
"/video/safety/events/statuses"

table_name = f"bronze.lytx_{endpoint.split('/')[-1]}_table"
        print(f"Creating table {table_name}...")
        process_api_data_to_delta(endpoint, table_name)
