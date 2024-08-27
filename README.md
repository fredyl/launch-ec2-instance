AttributeError: 'str' object has no attribute 'joint'
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
File <command-1374540070361399>, line 4
      1 table_name = "dev.bronze.pbi_dataflows_datasource"
      2 primary_key = ["datasourceId"]
----> 4 create_or_update_table_1(headers,rows,table_name,primary_key)

File <command-1374540070361392>, line 19, in create_or_update_table_1(headers, rows, table_name, primary_key_columns)
     15     existing_df = spark.table(table_name)
     17     df.createOrReplaceGlobalTempView('new_data')
---> 19     merge_condition = " AND ".joint([f"t.{col} = n.{col}" for col in primary_key_columns])
     21     merge_query = f"""
     22     MERGE INTO {table_name} AS t
     23     USING new_data AS n
   (...)
     28      THEN INSERT *
     29     """
     30 else: print(f"Table {table_name} does not exist. Creating a new table.")

AttributeError: 'str' object has no attribute 'joint'
