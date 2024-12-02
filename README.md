[UC_VOLUME_NOT_FOUND] Volume `dev`.`bronze_vendor`.`nightlyfiles_202411_backup` does not exist. Please use 'SHOW VOLUMES' to list available volumes. SQLSTATE: 42704
File <command-4075625855956794>, line 15
     12     with ThreadPoolExecutor() as executor:
     13         executor.map(backup_file, file_paths)
---> 15 create_backup_file(file_paths)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
