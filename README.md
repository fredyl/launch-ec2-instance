end_date = datetime.now().date()
table_name = f"bronze.lytx_video_events"
limit = 100
page = 1
all_events=[]
# complex_columns = ['deviceViews'] 
complex_columns = [field.name for field in df.schema.fields if isinstance(field.dataType,   
     (ArrayType, MapType))]
    
if spark.catalog.tableExists(table_name):
    df_table = spark.table(table_name)
    last_update = df_table.agg(F.max("update_time")).collect()[0][0]
    start_date = last_update.date()
else:
    start_date = end_date - timedelta(days=90)


while True:
    endpoint = f"/video/events?StartDate={start_date}&EndDate={end_date}&PageNumber={page}&PageSize={limit}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    print(f"getting data for page:{page}")
    print(response_data)
