if prev_volume_2 in volume_names:
    sql_spark= f"DROP VOLUME IF EXISTS {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"
    spark.sql(sql_spark)

elif prev_volume in volume_names:
    sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.nightlyfiles_{prev_month}_backup RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"""
    spark.sql(sql_spark)
elif main_volume in volume_names:
    sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.nightlyfiles RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month}_backup"""
    spark.sql(sql_spark)
elif main_volume not in volume_names:
    sql_spark= f"CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.nightlyfiles"
    spark.sql(sql_spark)
else:
    print("No previous month backup volume found")
