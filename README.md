ttributeError: 'str' object has no attribute 'startwith'
---------------------------------------------------------------------------
AttributeError                            Traceback (most recent call last)
File <command-1723439101937786>, line 33
     31 data_list = []
     32 for row in result_df.collect():
---> 33     if row.data and not row.data.startwith("Error") and not row.data.startswith("Exception"):
     34         try:
     35             page_data = json.loads(row.data).get(data_key, [])

AttributeError: 'str' object has no attribute 'startwith'
