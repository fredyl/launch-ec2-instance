# Get the column names for updating and inserting
        columns = spark_df.columns
        update_columns = ", ".join([f"t.{col} = n.{col}" for col in columns])
        insert_columns = ", ".join([f"n.{col}" for col in columns])

        merge_condition = " AND ".join([f"t.{col} = n.{col}" for col in primary_key])

        merge_query = f"""
        MERGE INTO {table_name} AS t
        USING new_data AS n
        ON {merge_condition}
        WHEN MATCHED THEN 
        UPDATE SET {update_columns}
        WHEN NOT MATCHED THEN 
        INSERT ({", ".join(columns)}) VALUES ({insert_columns})
        """

        spark.sql(merge_query)
