
if prev_month_2 in volume_names:
    sql_spark= f"DROP VOLUME IF EXISTS {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"
    spark.sql(sql_spark)
elif prev_month in volume_names:
    sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.nightlyfiles_{prev_month}_backup RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"""
    spark.sql(sql_spark)
elif len(source_volume) != 0 and source_volume in volume_names:
    sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.nightlyfiles RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month}_backup"""
    spark.sql(sql_spark)
else:
    print("No previous month to backup for {prev_month}")
