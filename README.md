table_columns = [f"`{col}`" if "@" in col else col for col in spark.table(table_name).columns]

insert_columns = ", ".join(table_columns)
insert_values = ", ".join([f"n.{col}" for col in table_columns])

update_columns = ", ".join([f"t.`{col}` = n.`{col}`" if "@" in col else f"t.{col} = n.{col}" for col in table_columns if col != "InsertTime"])

merge_query = f"""
MERGE INTO {table_name} AS t
USING new_data AS n
ON {merge_condition}
WHEN MATCHED THEN 
UPDATE SET {update_columns}
WHEN NOT MATCHED THEN 
INSERT ({insert_columns})
VALUES ({insert_values})
"""
