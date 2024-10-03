Exception: ('Failed:', 204, '')
File <command-1663170670380898>, line 64
     61         df.write.format("delta").mode("overwrite").saveAsTable(table_name)
     62         print(f"Table {table_name} created successfully with new data.")
---> 64 fetch_events_meta_data()
