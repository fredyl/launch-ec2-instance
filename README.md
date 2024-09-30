[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
when multiple source rows match on the same target row, the result may be ambiguous
as it is unclear which source row should be used to update or delete the matching
target row. You can preprocess the source table to eliminate the possibility of
multiple matches. Please refer to
https://docs.microsoft.com/azure/databricks/delta/merge#merge-error SQLSTATE: 21506
File <command-279195712476814>, line 37
     26     print(f"Table {table_name} exists. Performing upsert (merge)...")
     27     delta_table = DeltaTable.forName(spark, table_name)  # Reference the Delta table in the database
     28     delta_table.alias("existing_data") \
     29         .merge(df.alias("new_data"), F.expr(f"new_data.{primary_key} = existing_data.{primary_key}")) \
     30         .whenMatchedUpdate(set={
     31             "update_time": current_time, 
     32             **{col: F.col(f"new_data.{col}") for col in df.columns if col != primary_key}
     33         }) \
     34         .whenNotMatchedInsert(values={
     35             **{col: F.col(f"new_data.{col}") for col in df.columns}
     36         }) \
---> 37         .execute()
     38     print(f"Upsert (merge) completed successfully for {table_name}.")
     39 else:
