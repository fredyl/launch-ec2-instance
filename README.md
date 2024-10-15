[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve new_data.`<function col at 0x7f54ef5c3d80>` in INSERT clause given columns 


[DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
when multiple source rows match on the same target row, the result may be ambiguous
as it is unclear which source row should be used to update or delete the matching
target row. You can preprocess the source table to eliminate the possibility of
multiple matches. Please refer to


from delta.tables import DeltaTable
from pyspark.sql.functions import col, expr, current_timestamp
import json

def Holman_Upsert_data(data_type, code_key, data_key, table_name, primary_key=None):
    
    endpoint = f"{data_type}?{code_key}={code}&pageNumber={page}"
    table_name = f"bronze.holman_{data_type}"
    data_list = fetch_Holman_code_data(data_type, code_key, data_key,code)
    updated_data = replace_null_values(data_list)
    # print(json.dumps(updated_data, indent =2))
    df = spark.createDataFrame(updated_data)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if  col_name != primary_key and col_name != "tg_inserted"
                ]),  # Set tg_updated to current timestamp and update other columns from new data
                set={ "tg_updated": current_time, 
                    **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col != "tg_inserted"}}
            ) \
            .whenNotMatchedInsert(
                values={**{col_name: col(f"new_data.{col_name}") for col_name in df.columns}}
            ) \
            .execute()
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)
