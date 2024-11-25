[UC_VOLUME_NOT_FOUND] Volume `dev`.`bronze_vendor`.`nightlyfiles_backup` does not exist. Please use 'SHOW VOLUMES' to list available volumes. SQLSTATE: 42704
File <command-2757515768948675>, line 8
      5       backup_path = file_path.replace(base_path,f"{base_path}_backup").replace(f".csv",f"_bkup.csv")
      6       dbutils.fs.mv(file_path, backup_path)
----> 8 create_backUp_file(base_path,file_paths)
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
