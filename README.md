An error occurred: 
[PARSE_SYNTAX_ERROR] Syntax error at or near '.'. SQLSTATE: 42601 (line 4, pos 102)

== SQL ==

                    MERGE INTO groups AS target
                    USING temp_view AS source
                    ON target.d = source.d AND target.e = source.e AND target.v = source.v AND target.. = source.. AND target.b = source.b AND target.r = source.r AND target.o = source.o AND target.n = source.n AND target.z = source.z AND target.e = source.e AND target.. = source.. AND target.p = source.p AND target.b = source.b AND target.i = source.i AND target._ = source._ AND target.g = source.g AND target.r = source.r AND target.o = source.o AND target.u = source.u AND target.p = source.p AND target.s = source.s
------------------------------------------------------------------------------------------------------^^^
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *


 data_df.createOrReplaceTempView("temp_view")
            if isinstance(primary_keys, list):
                merge_condition = " AND ".join([f"target.{key} = source.{key}" for key in primary_keys]) 
            else: 
                # merge_condition = f"target.{primary_keys} = source.{primary_keys}"
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_keys])
                    
                merge_query = f"""
                    MERGE INTO {table_name} AS target
                    USING temp_view AS source
                    ON {merge_condition}
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
                    """

                

                spark.sql(merge_query)

        else:
                print(f"Table {table_name} does not exist. Creating a new table.")
                data_df.write.mode("overwrite").saveAsTable(table_name)
        return data_df

    except Exception as e:
        print(f"An error occurred: {str(e)}")
