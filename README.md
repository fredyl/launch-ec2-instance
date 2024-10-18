

%sql
SELECT cast(MAX(LastUpdated) as TIMESTAMP) MLU FROM bronze_vendor.LeadExec



## Prepare the variables used in the notebook
key = dbutils.secrets.get(scope="BIHubScope",key="LeadExecKey")
LID = '271'
StartDate = _sqldf.first()[0].strftime("%m-%d-%Y %H:%M:%S")
EndDate = datetime.now().strftime('%m-%d-%Y %H:%M:%S')
stagingPath = f'/Volumes/{env}/bronze_vendor/leadexec/'


## Print Start/End date so we can easily see it on each run
print("StartDate:\t", StartDate)
print("EndDate:\t", EndDate)

%sql
CREATE VOLUME IF NOT EXISTS bronze_vendor.LeadExec

def getQueryTotal(startDate, endDate, LID, key):
    ## Configure URL and take just one result (Fast) and ask for JSON
    url = f'http://apidata.leadexec.net/?key={key}&LID={LID}&StartDate={StartDate}&EndDate={EndDate}&Skip=0&Take=1'
    headers = {
        'Accept': 'application/json'
    }

    ## Get the response and convert it into JSON
    response = requests.get(url, headers=headers)
    rspjsn = json.loads(response.text)

    ## Pull the QueryTotal out of the response and return it
    tot = int(rspjsn['QueryTotal'])
    return tot



    queryTotal = getQueryTotal(StartDate, EndDate, LID, key)
loops = int(queryTotal / 1000) + 1

if queryTotal == 0:
    raise(Exception(f'LeadExec has returned 0 records between {StartDate} and {EndDate}'))



print(queryTotal, loops)



dataArray = []

for loop in range(0, loops):
    url = f'http://apidata.leadexec.net/?key={key}&LID={LID}&StartDate={StartDate}&EndDate={EndDate}&Skip={loop*1000}&Take=1000'
    dataArray.append({"Skip": loop*1000, "url": url})

fullQueryDF = spark.createDataFrame(dataArray)
fullQueryDF = fullQueryDF.withColumn('part', floor(col('Skip')/10000)).repartition(loops, 'part')
print(fullQueryDF.rdd.getNumPartitions())



def queryLeadsPartitioned(url, skip):
    ## Prepare query & get response
    headers = {
        'Accept': 'application/json'
    }
    response = requests.get(url, headers=headers)
    filePath = f'{stagingPath}LeadExec-{skip}.json'

    ## If we get a bad response, return that message.
    if response.status_code != 200:
        return "Bad Response"

    ## Otherwise, write the response to a file
    with open(filePath, 'w') as f:
        f.write(response.text)
    
    ## Return the file path to the dataframe
    return filePath

## Prepare the UDF
queryLeadsUDF = udf(lambda x, y: queryLeadsPartitioned(x, y), StringType())


outputData = fullQueryDF.withColumn('DataOutput', queryLeadsUDF(col('url'), col('skip'))).cache()
display(outputData)



badData = outputData.filter(col('DataOutput') == 'Bad Response').cache()
while badData.count() > 0:
    print(badData.count())
    badData = badData.withColumn('DataOutput', queryLeadsUDF(col('url'), col('skip'))).cache()
    badData = badData.filter(col('DataOutput') == 'Bad Response')



outputDF = spark.read.json(f'{stagingPath}*.json') \
  .select(explode('Leads').alias('Leads')).select('Leads.*') \
  .withColumn('tg_inserted', current_timestamp()) \
  .withColumn('tg_updated', current_timestamp())

display(outputDF)
# print(outputDF.count())




tableExists = spark.catalog.tableExists('bronze_vendor.LeadExec')

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


