
how can I use the below api endpoints to make a successful API get request with the below codes. Since I keep having an error 404

"/videos/safety/events/behaviors",
    "/videos/safety/events/triggersubtypes",
    "/videos/safety/events/triggers",
    "/videos/safety/eventsWithMetadata",
    "/video/vehicles/",
    "/vehicles/statuses",
    "/vehicles/types",
    "/vehicles"


def lytx_get_repoonse_from_event_api(endpoint):
    base_url = "https://lytx-api.prod5.ph.lytx.com"
    url = base_url + endpoint
    headers = {
        'x-apikey': api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        return response_data
    else:
        raise Exception("Failed:", response.status_code, response.text)


def process_api_data_to_delta(endpoint, table_name):
    response_data = lytx_get_repoonse_from_event_api(endpoint)

    if isinstance(response_data, list):
        df = spark.createDataFrame(response_data) 
    else:
        rdd = spark.sparkContext.parallelize([response_data])
        df = spark.read.json(rdd)
    current_time = F.current_timestamp()
    df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)

    if "id" in df.columns:
        primary_key = "id"
    else:
        raise Exception(f"No id column found in data schema for enppoint: {endpoint}")

    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name) 
        delta_table.alias("existing_data") \
          .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
          .whenMatchedUpdate(set={
              "update_time": current_time, 
              **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
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



endpoints =[
    "/safety/events/statuses",
    "/safety/events/behaviors",
    "/safety/events/triggersubtypes",
    "/safety/events/triggers"
]

for endpoint in endpoints:
    try:
        table_name = f"bronze.lytx_{endpoint.split('/')[-1]}_table"
        print(f"Creating table {table_name}...")
        process_api_data_to_delta(endpoint, table_name)
    except Exception as e:
        print(f"Error with {endpoint}: {e}")



def statuses_api_response():
    endpoint = "/videos/safety/events/triggers"   
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    return response_data
    print(response_data)
statuses_api_response()

Exception: ('Failed:', 404, '{"message":"no Route matched with those values"}')
