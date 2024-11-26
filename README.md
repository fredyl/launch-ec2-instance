if prev_month not in volume_names:
    sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.nightlyfiles RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month}_backup"""
    spark.sql(sql_spark)
else:
    print("No previous month to backup for {prev_month}")


    [VOLUME_ALREADY_EXISTS] Cannot create volume `dev`.`bronze_vendor`.`nightlyfiles` because it already exists.
Choose a different name, drop or replace the existing object, or add the IF NOT EXISTS clause to tolerate pre-existing objects. SQLSTATE: 42000
File <command-2757515768948719>, line 3
      1 if prev_month not in volume_names:
      2     sql_spark= f"""ALTER VOLUME  {env}.bronze_vendor.nightlyfiles RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month}_backup"""
----> 3     spark.sql(sql_spark)
      4 else:
      5     print("No previous month to backup for {prev_month}")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
