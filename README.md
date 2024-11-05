if tableExists != True:
    print("Table Doesn't Exist")
    outputDF.write.mode('overwrite').format('delta').saveAsTable('bronze_vendor.LeadExec')
    print("Table created!")
    print(f"Records Produced: {outputDF.count()}")
else:
    print("Table already exists!")
    rmoveDF = outputDF.join(table('bronze_vendor.LeadExec').select('UID', col('tg_inserted').alias('OriginalInsertTime')), 'UID', 'inner')
    rmove = rmoveDF.select('UID').collect()
    uids = []
    for r in rmove:
        ## We wrap the UID's in single quotes because they are injected below in the delete statement below as SQL Strings
        uids.append("'"+r[0]+"'")
    print(len(uids), ' UIDs matched!') 
    if len(uids) > 0:
        outputDF = outputDF.join(rmoveDF, 'UID', 'left') \
            .withColumn('tg_inserted', when(col('OriginalInsertTime').isNotNull(), col('OriginalInsertTime')).otherwise(col('tg_inserted'))) \
            .drop('OriginalInsertTime').cache()
        ## Delete records that exist in target table
        spark.sql(f'DELETE FROM bronze_vendor.LeadExec WHERE  UID in ({",".join(uids)})')
    ## We have to use append & mergeSchema here instead of a real 'merge' because
    ## new columns can come-up in nested data sources and we are handling them.
    outputDF.write.mode('append').format('delta').option('mergeSchema', True).saveAsTable('bronze_vendor.LeadExec')
    print(f"Records inserted: {outputDF.count()}")
## Delete the staging directory 
for f in dbutils.fs.ls(stagingPath):
    dbutils.fs.rm(f.path, True)
