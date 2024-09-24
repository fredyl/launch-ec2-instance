


dateLoop = getDateLoop(startDateTime, endDateTime)

for (start, end) in dateLoop:
    print(start,end)
    lastResultSet = None
    continuationToken = None
    lastResultSet, continuationToken = getResultForQuery(access_token, start=start, end=end)
    loopNumber = 1
    while lastResultSet == False:
        lastResultSet, continuationToken = getResultForQuery(access_token, start=start, ct=continuationToken, ln=loopNumber)
        loopNumber += 1

"""
Creating a dataframe from the json data and saving it to delta table
"""
timestamp = dt.utcnow()

file_path = f'/Volumes/{env}/bronze/powerbi/*.json'
df = spark.read.json(file_path)
df = df.withColumn('LastModified', lit(timestamp)) \
    .withColumn('insertTime', lit(timestamp))#add last inserted time with the curent timestamp
df = fixEmptyArrays(df)

if tableExists != True:
    print('Table Doesnt Exist')
    df.write.mode('overwrite').format('delta').saveAsTable('bronze.pbi_activity_event')
else:
    print('Table Exists')
    rmoveDF = df.join(table('bronze.pbi_activity_event')\
                        .withColumn('OriginalInsertTime', col('InsertTime')) \
                        .select('id', 'OriginalInsertTime').cache(), 'ID', 'inner')
    rmoveDF = spark.createDataFrame(rmoveDF.select('id', 'OriginalInsertTime').collect())
    rmove = rmoveDF.select('ID').collect()
    ids = []
    for r in rmove:
        ids.append("'"+r[0]+"'")
    if len(ids) > 0:
        print(f"Removing {len(ids)} old records.")
        df = df.join(rmoveDF.select('id', 'OriginalInsertTime'), 'id', 'left') \
            .withColumn('InsertTime', when(col('OriginalInsertTime').isNotNull(), col('OriginalInsertTime')).otherwise(col('InsertTime'))) \
            .drop('OriginalInsertTime').cache()
        spark.sql(f'DELETE FROM bronze.pbi_activity_event WHERE ID IN ({",".join(ids)})')
    print(f'Upserting {df.count()} records!')
    df.write.mode('append').format('delta').option('mergeSchema', True).saveAsTable('bronze.pbi_activity_event')

for f in dbutils.fs.ls(f'/Volumes/{env}/bronze/powerbi/'):
    dbutils.fs.rm(f.path)
