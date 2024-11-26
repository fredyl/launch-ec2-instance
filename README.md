if prev_volume_2 in volume_names:
    sql_spark= f"DROP VOLUME IF EXISTS {env}.bronze_vendor.{prev_volume_2}"
    spark.sql(sql_spark)

if prev_volume in volume_names:
    sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.{prev_volume} RENAME TO {env}.bronze_vendor.{prev_volume_2}"""
    spark.sql(sql_spark)
if main_volume in volume_names:
    sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.{main_volume} RENAME TO {env}.bronze_vendor.{prev_volume}"""
    spark.sql(sql_spark)
    
volumes = spark.sql(f"SHOW VOLUMES IN {env}.bronze_vendor").collect()
volume_names = [volume.volume_name for volume in volumes]
print(volume_names)
if main_volume not in volume_names:
    sql_spark= f"CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.nightlyfiles"
    spark.sql(sql_spark)
