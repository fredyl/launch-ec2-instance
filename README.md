AttributeError: 'FileInfo' object has no attribute 'chk_path'
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
File <command-2638019235405315>, line 91
     88     print(f"Data upserted for data type {data_type} with code value {code_value}")
     90 for f in dbutils.fs.ls(chk_path):
---> 91     dbutils.fs.rm(f.chk_path, True)

AttributeError: 'FileInfo' object has no attribute 'chk_path'
