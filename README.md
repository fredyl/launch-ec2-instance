from pyspark.sql.functions import col, sum as spark_sum

def check_dups_data(data_type, data_key, data_list, primary_key):
    
    table_name = f"bronze.holman_{data_type}_{data_key}"
    print(table_name)
    df = spark.createDataFrame(data_list)
    current_time = current_timestamp()
    df = df.withColumn("tg_inserted", current_time).withColumn("tg_updated", current_time)
    # Check for duplicates based on primary key

    if data_type == 'vehicles':
        compbined_key = "compbined_key"
        df = df.withColumn(compbined_key, concat(col(primary_key), lit("_"), col("deliveryDate")))
        primary_key = compbined_key
    elif data_type == 'maintainance':
        df = df.withColumn(compbined_key, concat(col(primary_key), lit("_"), col("invoiceDate")))
        primary_key = compbined_key
    else:
        pass

    dup_check_df = df.groupBy(primary_key).count().filter(col("count") > 1)
    dup_check_df.show()
    total_duplicates = dup_check_df.agg(spark_sum(col("count").cast("int"))).collect()[0][0]
    print(f"Total dubs {total_duplicates}")
    df = df.dropDuplicates([primary_key])
    return df

    # duplicate_id = [row[primary_key] for row in dup_check_df.collect()]
    # df = df.filter(~col(primary_key).isin(duplicate_id))

def Holman_Upsert_data(data_type, data_key, data_list, primary_key=None):

    df = check_dups_data(data_type, data_key, data_list, primary_key)
    if spark.catalog.tableExists(table_name):
        print(f"Table {table_name} exists. Performing upsert (merge)...")
        delta_table = DeltaTable.forName(spark, table_name)
        delta_table.alias("existing_data") \
            .merge(
                df.alias("new_data"), expr(f"new_data.{primary_key} = existing_data.{primary_key}")
            ) \
            .whenMatchedUpdate(
                condition=" OR ".join([
                    f"existing_data.{col_name} != new_data.{col_name}" for col_name in df.columns 
                    if col_name != primary_key and col_name != "tg_inserted"
                ]),
                set={
                     **{col_name: col(f"new_data.{col_name}") for col_name in df.columns if col_name != primary_key and col_name != "tg_inserted"}}) \
            .whenNotMatchedInsertAll() \
            .execute()
        print(f"Upsert completed for {table_name}")
    else:
        print(f"Table does not exists, creating new table {table_name}")
        df.write.format("delta").saveAsTable(table_name)


#define a set of endpoints with corresponding data keys
holman_endpoints = {
    "vehicles": ("inventory","clientVehicleNumber"),
    "maintenance": ("maintenance","clientVehicleNumber"),
    "accidents": ("accident","record_id"),
    "Odometer": ("odometerHistory","vin"),
    "persons": ("person","contactEmployeeId")
}

#iterate through the endpoints to fetch and upsert data
for data_type,( data_key, primary_key) in holman_endpoints.items():
    data_list = get_holman_data(token, data_type =data_type, data_key=data_key)
    data_list = replace_null_values(data_list)
    # print(json.dumps(data_list, indent=4))
    if data_list:
        Holman_Upsert_data(data_type, data_key, data_list, primary_key)
    else:
        print(f"No data found for {data_type}_{data_key}")
else:
    print("All data fetched")



[DELTA_MERGE_UNRESOLVED_EXPRESSION] Cannot resolve leaseTerm in UPDATE clause given columns existing_data.ariVehicleNumber, existing_data.assetType, existing_data.auxData1, existing_data.auxData10, existing_data.auxData11, existing_data.auxData12, existing_data.auxData13, existing_data.auxData14, existing_data.auxData2, existing_data.auxData3, existing_data.auxData4, existing_data.auxData5, existing_data.auxData6, existing_data.auxData7, existing_data.auxData8, existing_data.auxData9, existing_data.auxDate1, existing_data.auxDate2, existing_data.auxDate3, existing_data.auxDate4, existing_data.bodyPoNumber, existing_data.chassisAtUpfitterDate, existing_data.clientData1, existing_data.clientData2, existing_data.clientData3, existing_data.clientData4, existing_data.clientData5, existing_data.clientData6, existing_data.clientData7, existing_data.clientVehicleNumber, existing_data.dealerAddress, existing_data.dealerAddress2, existing_data.dealerCity, existing_data.dealerPaperMailed, existing_data.dealerPostal, existing_data.dealerStateProv, existing_data.deliveredToDealerDate, existing_data.deliveryDate, existing_data.deliveryDealer, existing_data.division, existing_data.driverClass, existing_data.estimatedDelivDate, existing_data.estimatedSchedProdDate, existing_data.exec, existing_data.firstName, existing_data.invoiceDate, existing_data.lastName, existing_data.lesseeCode, existing_data.makeClient, existing_data.makeVin, existing_data.mfgAcknowledgedDate, existing_data.mfgOrderLockedIn, existing_data.mfgOrderNumber, existing_data.modelClient, existing_data.modelVin, existing_data.modelYear, existing_data.notifiedOfDelivery, existing_data.orderPlacedDate, existing_data.orderReceivedDate, existing_data.orderStatus, existing_data.orderStatusDate, existing_data.orderType, existing_data.prefix, existing_data.previousVehicle, existing_data.producedDate, existing_data.scheduledProductionDate, existing_data.shipDate, existing_data.titleLocation, existing_data.upfitChassisShippedDate, existing_data.upfitCompleteDate, existing_data.upfitInvoiceDate, existing_data.upfitPoIssueDate, existing_data.vin, existing_data.whoWillLicense, existing_data.tg_inserted, existing_data.tg_updated. SQLSTATE: 42601
File <command-310280906946883>, line 16
     14 # print(json.dumps(data_list, indent=4))
     15 if data_list:
---> 16     Holman_Upsert_data(data_type, data_key, data_list, primary_key)
     17 else:
     18     print(f"No data found for {data_type}_{data_key}")
File /databricks/spark/python/pyspark/errors/exceptions/captured.py:261, in capture_sql_exception.<locals>.deco(*a, **kw)
    257 converted = convert_exception(e.java_exception)
    258 if not isinstance(converted, UnknownException):
    259     # Hide where the exception came from that shows a non-Pythonic
    260     # JVM exception message.
--> 261     raise converted from None
    262 else:
    263     raise
