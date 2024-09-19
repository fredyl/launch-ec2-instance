merge_condition = " AND ".join([f"t.{col} = n.{col}" if "@" not in col else f"t.`{col}` = n.`{col}`" for col in primary_key])
