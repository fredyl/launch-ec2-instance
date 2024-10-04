
def load_checkpoint():
    if os.path.exists("checkpoint.json"):
        with open("checkpoint.json", "r") as f:
            checkpoint_data = json.load(f)
            return checkpoint_data.get("last_processed_page", 1)
    else:
        page = 1
def save_checkpoint(page):
    with open("checkpoint.json", "w") as f:
        json.dump({"last_processed_page": page,}, f) 

        
def fetch_events_meta_data():
    page = load_checkpoint()
    from_date = "2023-07-31T16:19:58.0000Z"
    to_date = "2024-03-31T16:19:58.0000Z"
    limit = 1000
    all_events_metadata=[]
    pages_to_comit = 25
    complex_columns = ['behaviors','coachingSessionNotes','eventNotes','notes', 'reviewNotes'] 

    while True:
        endpoint =f"/video/safety/eventsWithMetadata?from={from_date}&to={to_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
        response,response_data = lytx_get_repoonse_from_event_api(endpoint)
        # print(f"getting data for page:{page}")
        status_code = response.status_code

        if response_data is None:
            print(f" recieved 204, No Content on page {page}, Stopping Pagination")
            break

        if status_code == 200 and response_data:
            print(f"Processing page{page}, with {len(response_data)} records")
            all_events_metadata.extend(response_data)
        else:
            break
        

        if page % pages_to_comit == 0 or response_data is None:
            if all_events_metadata:
                df = spark.createDataFrame(all_events_metadata)  
                current_time = F.current_timestamp()
                df = df.withColumn("insert_time", current_time).withColumn("update_time", current_time)
                
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
                            [f"existing_data.{col} != new_data.{col}" for col in df.columns if col not in complex_columns and col != primary_key and col != "insert_time"]),
                            set={col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key and col != "insert_time"}
                            ) \
                        .whenNotMatchedInsert(values={col: F.col(f"new_data.{col}") for col in df.columns}
                        ) \
                        .execute()
                    print(f"Upsert (merge) completed successfully for {table_name}.")
                else:
                    print(f"Table {table_name} does not exist. Creating new table...")
                    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
                    print(f"Table {table_name} created successfully with new data.")

                all_events_metadata=[]

                save_checkpoint(page)

                print(f"pausing after processing page {page}.")
                return
        page +=1
    print("Pagination completed")
fetch_events_meta_data()
