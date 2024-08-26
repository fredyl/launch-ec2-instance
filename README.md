 def create_view_and_push_data_to_database(self, api_endpoint, table_name):
        datasets_df = self.create_data_frame(api_endpoint)
        datasets_df.createOrReplaceTempView("dataflow_temp")
        # table_name = self.table_name

        pbi_datasets_exists = spark.sql(f"SHOW TABLES IN dev.bronze LIKE '{table_name}'").count() > 0

        if not pbi_datasets_exists:
            datasets_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(table_name)       
        else:
            #Merge the new data from dataflow_temp to the already existing pbi_dataflows table
            spark.sql(f"""MERGE INTO '{table_name}' AS target
            USING datasets_temp AS source
            ON target.id = source.id 
            WHEN MATCHED THEN
            UPDATE SET *
            WHEN NOT MATCHED THEN
            INSERT * """ )


    def create_data_frame(self, api_endpoint):
        group_ids = self.get_groups_id()
        combined_df = None
        for group_id in group_ids:
            data_json_data = self.get_json_data(group_id, api_endpoint)
            data_rdd = spark.sparkContext.parallelize([json.dumps(data_json_data['value'])])
            data_df = spark.read.json(data_rdd)
            
            if combined_df is None:
                combined_df = data_df
            else:
                combined_df = combined_df.unionByName(data_df, allowMissingColumns=True)
        return combined_df
