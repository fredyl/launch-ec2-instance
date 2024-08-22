# Convert dataflow_refresh_data to an RDD and read it as a JSON DataFrame
dataflow_refresh_data_rdd = self.spark.sparkContext.parallelize([json.dumps(dataflow_refresh_data)])
dataflow_refresh_data_df = self.spark.read.json(dataflow_refresh_data_rdd)

# Debugging: Check if DataFrame is properly created
if dataflow_refresh_data_df is not None and len(dataflow_refresh_data_df.head(1)) > 0:
    dataflow_refresh_data_df.show(truncate=False)
    dataflow_refresh_data_df = self.extract_and_flatten_refresh_attempt(dataflow_refresh_data_df)
    try:
        dataflow_refresh_data_df.createOrReplaceTempView("dataflow_refresh_temp")
        print("Temp View dataflow_refresh_temp is created")
    except Exception as e:
        print(f"Failed to create temp view: {e}")
        return

    try:
        # Check if the table exists
        table_exists = self.spark.sql("SHOW TABLES IN dev.bronze LIKE 'pbi_dataset_refreshes'").count() > 0

        if not table_exists:
            print("Table does not exist. Creating table...")
            dataflow_refresh_data_df.write.mode("overwrite").saveAsTable('dev.bronze.pbi_dataset_refreshes')
            print("Table 'pbi_dataset_refreshes' created successfully.")
        else:
            print("Table exists. Merging data...")
            self.spark.sql(f"""
                MERGE INTO dev.bronze.pbi_dataset_refreshes AS target
                USING dataflow_refresh_temp AS source
                ON target.requestId = source.requestId  
                WHEN MATCHED THEN
                UPDATE SET *
                WHEN NOT MATCHED THEN
                INSERT *
            """)
            print("Data merged successfully into 'pbi_dataset_refreshes'.")
    except Exception as e:
        print(f"Error during table creation or data merging: {e}")
else:
    print("DataFrame is empty or was not created properly.")
