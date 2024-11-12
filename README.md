TypeError: unsupported operand type(s) for -: 'str' and 'datetime.timedelta'
---------------------------------------------------------------------------
TypeError                                 Traceback (most recent call last)
File <command-3021351257305334>, line 23
     21     print(f"end date: {end_date}")
     22 else:
---> 23     start_date = (end_date - timedelta(days=90)).date().isoformat()
     24 #pagination
     25 all_events = get_lytx_paging_data(endpoint, page, limit, start_date, end_date)

TypeError: unsupported operand type(s) for -: 'str' and 'datetime.timedelta'


#Handles video events API with complex columns pagination and upsert


end_date = datetime.now().date().isoformat()
table_name = "bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]
endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
 

#check if the delta table exists to determine the start date  for fetching data
#aggregate find the maximum value of tg_updated column, which represents most updated time
#collect()[0][0] extracts the single value for the max tg_updated column from aggregate results
if spark.catalog.tableExists(table_name): 
    df_table = spark.table(table_name)
    last_update = df_table.agg(max("tg_updated")).collect()[0][0]
    print(f"last update time: {last_update}")
    start_date = last_update.date().isoformat()     #let the start date be the last update time
    print(f"start date: {start_date}")
    print(f"end date: {end_date}")
else:
    start_date = (end_date - timedelta(days=90)).date().isoformat()
#pagination
all_events = get_lytx_paging_data(endpoint, page, limit, start_date, end_date)
#convert data to dataframe and upsert to the table
if all_events:
    df = spark.createDataFrame(all_events)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time,['deviceViews'] )
    
else:
    print("No new data found")




def upsert_data(df, table_name, current_time, complex_columns=[], primary_key="id", endpoint=None):
    '''
    Generic Function that performs an upsert
    '''
    
    # Setting the value of [] to an empty list if not provided
    # complex_columns = complex_columns or []

    # Check if primary_key exist in dataframe column. If not raises an exception
    if primary_key not in df.columns:
        raise Exception(f"No id column found in data schema for endpoint: {endpoint}")

    if spark.catalog.tableExists(table_name):  # Check if table exists
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database

        #separate the columns into non_complex and complex columns for update
        non_complex_columns = [col_name for col_name in df.columns
                if col_name not in complex_columns and col_name != primary_key and col_name != "tg_inserted"]
        
        # Performing merge operation between existing data and new data
        #defining columns to be updated, when data matched update existing data
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                #update only non_complex_columns by comparing values
                #always update complex_column without comparing value since complex columns are nested data
                set={ 
                    **{col_name: col(f"new_data.{col_name}") for col_name in non_complex_columns},
                    **{col_name: col(f"new_data.{col_name}") for col_name in complex_columns},
                }
            ) \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Upsert (merge) completed successfully for {table_name}.")
    
    #create a new table if table does not exists
    else:
        print(f"Table {table_name} does not exist. Creating new table...")
        df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"Table {table_name} created successfully with new data.")
