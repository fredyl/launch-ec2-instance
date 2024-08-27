if isinstance(primary_keys, list):
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys])
            else:
                merge_condition = f"target.{primary_keys} = source.{primary_keys}"
