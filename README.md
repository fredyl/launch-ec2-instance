Exception: No id column found in data schema for enppoint: /video/safety/eventsWithMetadata?from=2023-01-01T16:19:58.0000Z&to=2023-02-02T16:19:58.0000Z&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit=1000&page=28
---------------------------------------------------------------------------
Exception                                 Traceback (most recent call last)
File <command-1663170670380898>, line 70
     67         df.write.format("delta").mode("overwrite").saveAsTable(table_name)
     68         print(f"Table {table_name} created successfully with new data.")
---> 70 fetch_events_meta_data()

File <command-1663170670380898>, line 47, in fetch_events_meta_data()
     45     primary_key = "id"
     46 else:
---> 47     raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
     48 table_name = f"bronze.lytx_video_eventsWithMetadata"
     49 if spark.catalog.tableExists(table_name):

Exception: No id column found in data schema for enppoint: /video/safety/eventsWithMetadata?from=2023-01-01T16:19:58.0000Z&to=2023-02-02T16:19:58.0000Z&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit=1000&page=28
