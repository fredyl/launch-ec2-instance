delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if col_name != primary_key and col_name != "tg_inserted"
                ]),
                set={
                     **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}}) \
            .whenNotMatchedInsertAll() \
            .execute()
