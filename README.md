[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve accountCode in INSERT clause given columns new_data.amountBilled, new_data.assetType, new_data.auxData1, new_data.auxData10, new_data.auxData11, new_data.auxData12, new_data.auxData13, new_data.auxData14, new_data.auxData2, new_data.auxData3, new_data.auxData4, new_data.auxData5, new_data.auxData6, new_data.auxData7, new_data.auxData8, new_data.auxData9, new_data.billLesseeData, new_data.bookValue, new_data.capCost, new_data.clientVehicleNumber, new_data.division, new_data.dueDate, new_data.fileNumber, new_data.firstBillIndicator, new_data.firstName, new_data.gstTax, new_data.insuranceAmount, new_data.interest, new_data.invoiceDate, new_data.invoiceNumber, new_data.lastBillIndicator, new_data.lastName, new_data.leaseTerm, new_data.lesseeCode, new_data.lesseeData1, new_data.lesseeData2, new_data.lesseeData3, new_data.lesseeData4, new_data.lesseeData5, new_data.lesseeData6, new_data.lesseeData7, new_data.licensePlate, new_data.makeName, new_data.managementFee, new_data.modelName, new_data.modelYear, new_data.monthsBilled, new_data.monthsInService, new_data.prefix, new_data.record_id, new_data.rental, new_data.stateProvince, new_data.taxAmount, new_data.vehicleNumber, new_data.vin, new_data.yearMonth, new_data.tg_inserted, new_data.tg_updated, new_data.combined_key. SQLSTATE: 42601
File <command-2638019235405315>, line 65
     58         data_list.extend(row[data_key])
     59     # print(json.dumps(data_list, indent=2  ))
     60     
     61 # if data_list:
     62 # # cleaned_data = replace_null_values(data_list)
     63 # #     cleaned_data = replace_null_values(data_list)
     64 #     # print(json.dumps(cleaned_data, indent=2  ))
---> 65 Holman_Upsert_data(data_type, data_key,data_list, primary_key)
     66 print(f"Data upserted for data type {data_type} with code value {code_value}")
     68 for f in dbutils.fs.ls(chk_path):
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
