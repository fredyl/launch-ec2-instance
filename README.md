def parallelize_holman_data_fetch(token, data_type, data_key, primary_key, partition_range=100, num_partitions=10):
    """
    Function to parallelize data fetching using mapPartitions.
    This function handles the partitioning and API call distribution.
    """
    # Define an RDD to simulate partitioning over a range
    rdd = spark.sparkContext.parallelize(range(0, partition_range), numSlices=num_partitions)

    # Use mapPartitions to fetch data in parallel across partitions
    partitioned_data = rdd.mapPartitions(
        lambda iterator: get_holman_data_partition(iterator, token, data_type, data_key, primary_key)
    )

    # Collect the fetched data
    data_list = partitioned_data.collect()
    return data_list


# Keep the Holman_Upsert_data function generic as requested
def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)

    # Create DataFrame from the data_list
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)

    # Check for duplicates based on primary key
    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.display()
    duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    df = df.filter(~col(primary_key).isin(duplicate_id))

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                set={col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}
            ) \
            .whenNotMatchedInsert(
                values={col_name: col(f"new_data.{col_name}") for col_name in df.columns}
            ) \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exist, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


# Fetching and upserting data with parallelization outside the generic function
for data_type, (data_key, primary_key) in holman_endpoints.items():
    print(f"Fetching and upserting data for {data_type}_{data_key}")
    data_list = parallelize_holman_data_fetch(token, data_type, data_key, primary_key)
    
    if data_list:
        # Perform the upsert after data is fetched
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
