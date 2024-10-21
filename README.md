def process_api_data_to_delta(endpoint, table_name):
    '''
    This generic function is used to process the data from the api and insert or update the data to the delta table. 
    '''
    response = lytx_get_repoonse_from_event_api(endpoint) # fetch data from lytx api
    response_data = response.json()
    print(len(response_data))
    if isinstance(response_data, list): #check if response data is a list or not and creates a dataframe
        df = spark.createDataFrame(response_data) 
    else:
        #if data is not a list Parallelize the data to a rdd
        rdd = spark.sparkContext.parallelize([response_data]) 
        df = spark.read.json(rdd)
        
    #get current time stamp and add the columns tg_updated and tg_inserted to the dataframe and perform an upsert to delta table
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    upsert_data(df,table_name,current_time, complex_columns=[],primary_key='id',endpoint=None)
