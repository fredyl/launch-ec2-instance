'''
The below codes handes the vehicles data with pagination and upsert
'''
limit = 100
page = 1
all_vechicles=[]

#defining the table name and endpoint
table_name = f"bronze.lytx_vehicles_vehicle_id"
endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"
all_vehicles = get_lytx_paging_data(endpoint, page, limit)
df = spark.createDataFrame(all_vehicles)

#create dataframe and get current time stamp and add the columns tg_updated and tg_inserted to the dataframe
#drop duplicates if exists and do upsert
current_time = current_timestamp()
df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
dup_check_df = df.groupBy("id").count().filter(col("count") > 1)
dup_check_df.show(truncate=False)
df_partitioned = df.repartition('id')
df_sorted = df_partitioned.sortWithinPartitions('id',desc('lastConnected'))
df = df_sorted.dropDuplicates(subset=['id'])
upsert_data(df,table_name,current_time,primary_key='id',endpoint=None)
