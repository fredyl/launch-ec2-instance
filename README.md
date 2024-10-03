def fetch_events_meta_data():
    page =1
    from_date = "2023-01-01T16:19:58.0000Z"
    to_date = "2023-01-30T16:19:58.0000Z"
    limit = 1000
    all_events_metadata=[]

    while True:
        endpoint =f"/video/safety/eventsWithMetadata?from={from_date}&to={to_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
        response,response_data = lytx_get_repoonse_from_event_api(endpoint)
        print(f"getting data for page:{page}")
        status_code = response.status_code

        if response_data is None:
            print(f" recieved 204, No Content on page {page}, Stopping Pagination")
            break

        if status_code == 200:
            response_data = response.json()
            all_events_metadata.extend(response_data)
            if not response_data:
                print(f"No data found on page {page}, Stopping Pagination")
                break
        else:
            print(f"Error Occured:{status_code}")
            break
        page +=1
    print("Pagination completed")

    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data) 
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)

    dup_check_df = df.groupBy("id").count().filter(F.col("count") > 1)
    dup_check_df.show(truncate=False)
    df_partitioned = df.repartition('id')
    df_sorted = df_partitioned.sortWithinPartitions('id',F.desc('lastConnected'))
    df = df_sorted.dropDuplicates(subset=['id'])

    df.display(truncate =False)

    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")
    table_name = f"bronze.lytx_video_eventsWithMetadata"
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
        delta_table.alias("existing_data") \
            .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
            .whenMatchedUpdate(condition= " OR ".join(
                [f"existing_data.{col} != new_data.{col}" for col in df.columns if col != primary_key and col != "insert_time"]),
                set={
                    "update_time": current_time, 
                    **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "insert_time"}
                }) \
            .whenNotMatchedInsert(values={
                **{col: F.col(f"new_data.{col}") for col in df.columns}
            }) \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")

fetch_events_meta_data()
