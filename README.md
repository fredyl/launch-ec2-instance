def get_enabled_views(control_table):
  return spark.sql(f"select * from {control_table} where enabled = true").collect()

enabled_views = get_enabled_views(control_table)
for views in enabled_views:
  print(views['sourceTable'])
