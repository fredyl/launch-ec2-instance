def create_or_update_dataflow_refreshes_table(self, table_name, dataflow_refresh_data):
            """
            Processes dataset refresh history data and updates or creates a corresponding table.
            """
            if not dataflow_refresh_data:
                print(f"No data to proicess for table{table_name}")

            #convert dataflow_refresh_data to an RDD and read it as JSON dataframe
            dataflow_refresh_data_rdd = self.spark.sparkContext.parallelize([dataflow_refresh_data])
            dataflow_refresh_data_df = self.spark.read.json(dataflow_refresh_data_rdd)
            # dataflow_refresh_data_df.show()  # Optional: Display the DataFrame for debugging
            
            #debuging Check if dataframe is porperly created 
            if dataflow_refresh_data_df is not None and len(dataflow_refresh_data_df.head(1)) > 0:
                dataflow_refresh_data_df.show(truncate=False)
                dataflow_refresh_data_df = self.extract_and_flattened_refresh_attempt(dataflow_refresh_data_df)
                try:
                    dataflow_refresh_data_df.createOrReplaceTempView("dataflow_refresh_temp")
                    print("Temp View dataflow_refresh_temp is created")
                except Exception as e:
                    print(f"Failed to create temp view : {e}")
                    return

                table_exists = self.spark.sql("SHOW TABLES IN dev.bronze LIKE '{pbi_dataflow_refreshes}'").count() > 0

                if not table_exists:
                    dataflow_refresh_data_df.write.mode("overwrite").saveAsTable('dev.bronze.pbi_dataset_refreshes')
                else:
                    spark.sql(f"""
                    MERGE INTO dev.bronze.pbi_dataset_refreshes AS target
                    USING dataflow_refresh_temp AS source
                    ON target.requestId = source.requestId  
                    WHEN MATCHED THEN
                    UPDATE SET *
                    WHEN NOT MATCHED THEN
                    INSERT *
                    """)
