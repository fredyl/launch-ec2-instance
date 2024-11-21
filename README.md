AttributeError: 'dict' object has no attribute 'select'
File <command-265156676446858>, line 1
----> 1 process_saved_views(control_table)
File <command-362886976688533>, line 6, in save_partition_full_loads(view_name, df, base_path)
      1 def save_partition_full_loads(view_name,df,base_path):
      2   '''
      3   The function will save full loads files grouped by YearMonth with optimized Partitioning
      4   '''
----> 6   YearMonth =data_df.select("YearMonth").distinct().rdd.flatmap(lambda x: x).collect()
      7   for yearmonth in YearMonth:
      8     partitioned_df = data_df.filter(data_df.YearMonth == yearmonth)
