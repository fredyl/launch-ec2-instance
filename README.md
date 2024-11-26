if prev_month_2:
    sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.nightlyfiles_{prev_month}_backup RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"""
    spark.sql(sql_spark)


[UC_VOLUME_NOT_FOUND] Volume `dev`.`bronze_vendor`.`nightlyfiles_202410_backup` does not exist. Please use 'SHOW VOLUMES' to list available volumes. SQLSTATE: 42704
File <command-2757515768948960>, line 3
      1 if prev_month_2:
      2     sql_spark= f"""ALTER VOLUME {env}.bronze_vendor.nightlyfiles_{prev_month}_backup RENAME TO {env}.bronze_vendor.nightlyfiles_{prev_month_2}_backup"""
----> 3     spark.sql(sql_spark)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
