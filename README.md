from pyspark.sql.functions import col, sum as spark_sum

def check_dups_data(data_type, data_key, data_list, primary_key):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # Check for duplicates based on primary key

    if data_type == 'vehicles':
        compbined_key = "compbined_key"
        df = df.withColumn(compbined_key, concat(col(primary_key), lit("_"), col("deliveryDate")))
        primary_key = compbined_key
    elif data_type == 'maintainance':
        df = df.withColumn(compbined_key, concat(col(primary_key), lit("_"), col("invoiceDate")))
        primary_key = compbined_key
    else:
        pass

    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.show()
    total_duplicates = dup_check_df.agg(spark_sum(col("count").cast("int"))).collect()[0][0]
    print(f"Total dubs {total_duplicates}")
    df = df.dropDuplicates([primary_key])
    return df

    # duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    # df = df.filter(~col(primary_key).isin(duplicate_id))

def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):

    df = check_dups_data(data_type, data_key, data_list, primary_key)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if col_name != primary_key and col_name != "tg_inserted"
                ]),
                set={
                     **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}}) \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


#define a set of endpoints with corresponding data keys
holman_endpoints = {
    "vehicles": ("inventory","clientVehicleNumber"),
    "maintenance": ("maintenance","clientVehicleNumber"),
    "accidents": ("accident","record_id"),
    "Odometer": ("odometerHistory","vin"),
    "persons": ("person","contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type,( data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
    data_list = replace_null_values(data_list)
    # print(json.dumps(data_list, indent=4))
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
else:
    print("All data fetched")
