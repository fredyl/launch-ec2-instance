# Databricks notebook source
from tglib import ErrorHandler
eh = ErrorHandler(sendEmail=False)
env = spark.catalog.currentCatalog()

# COMMAND ----------

# DBTITLE 1,Perform Imports and configure some params
import requests, msal, json, datetime
from pyspark.sql.functions import col, lit, when, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from delta.tables import DeltaTable

client_id = "dcb4b38c-e50a-49ec-a5a3-d5300184e4be"
tenant_id = "aadec23e-c40c-47d7-a647-2566ee1c67da"
client_secret = dbutils.secrets.get(scope="BIHubScope",key="databricks-powerbi-connector")
authority = f'https://login.microsoftonline.com/{tenant_id}'
scope = ['https://analysis.windows.net/powerbi/api/.default']

# COMMAND ----------

start = datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Fetching OAuth2 Token from Microsoft Azure API
url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

body = f'client_id={client_id}&grant_type=client_credentials&scope=https%3A%2F%2Fanalysis.windows.net%2Fpowerbi%2Fapi%2F.default&client_secret={client_secret}'

response = requests.get(url, headers=headers, data=body)
token = json.loads(response.text)['access_token']

# COMMAND ----------

def callAPI(token, endpoint, params=None):
    ## This function is used throughout the program to retrieve data from the PBI API
    url = 'https://api.powerbi.com/v1.0/myorg/' + endpoint
    headers= { 'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
    }
    ## Try to get a response up to 3 times, return None if we can't get a successful response.
    for _ in range(3):
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response.text
    return None

# COMMAND ----------

def getObjects(token, groupId):
    ## This function can be used to retrieve top level objects in a group

    ## Initialize output variable
    objectList = []

    ## Check Datasets, Reports, and Dataflows for each group. (Groups handled syncronously via UDF)
    for apiType in ['datasets', 'reports', 'dataflows']:
        endpt = f'/groups/{groupId}/{apiType}'
        response = callAPI(token, endpt)

        ## All PBI APIs return a JSON object with a 'value' key
        objects = json.loads(response)['value']

        ## For each object returned
        for o in objects:
            if apiType == 'dataflows':
                idkey = 'objectId'
            else:
                idkey = 'id'
            ## Put the objects data into the array that will be returned
            objectList.append({'name': o['name'], 'objectID': o[idkey], 'type': apiType})
        if len(objects) > 0:
            ## Write the full json response to a file to be processed later
            with open(f'/Volumes/{env}/bronze/powerbi/{groupId}_{apiType}.json', 'w') as f:
                f.write(response)
    if len(objectList) > 0:
        return objectList
    else:
        return None

## Utilie the getObjects function as a UDF so it can run in parallel per partition
getObjectsUDF = udf(lambda x: getObjects(token, x), ArrayType(StructType([StructField('name', StringType()), StructField('objectID', StringType()), StructField('type', StringType())])))

# COMMAND ----------

def getObjectData(token, groupId, objectId, objectType):
    ## This function is used to return the datasources & pages for each object type

    ## Initialize Key Variable
    if objectType in ['dataflows', 'datasets']:
        keyId = 'datasources'
    else:
        keyId = 'pages'

    response = callAPI(token, f'/groups/{groupId}/{objectType}/{objectId}/{keyId}')
    if response != None:
        ## Write the outputs to files to be read later
        with open(f'/Volumes/{env}/bronze/powerbi/{groupId}_{objectType}_{objectId}.json', 'w') as f:
            f.write(response)
        return 'OK'
    return 'ERROR'

## Create a UDF for getObjectData so it can be run in parallel
getObjectDataUDF = udf(lambda x, y, z: getObjectData(token, x, y, z), StringType())

# COMMAND ----------

def processObjects():
    ## Function for processing returned JSON data into dataframes
    tstamp = datetime.datetime.utcnow().isoformat()
    objects = {
        'datasets': [],
        'reports': [],
        'dataflows': []
    }
    ## Iterate through each file
    for fil in dbutils.fs.ls(f'/Volumes/{env}/bronze/powerbi'):
        fsplit = fil.name.split('_')
        groupId = fsplit[0]
        dType = fsplit[1].replace('.json', '')
        oid = None
        ## This will happen for second-level items (Datasources & Pages)
        if len(fsplit) > 2:
                oid = fsplit[2].replace('.json', '')
        
        ## Read the JSON from the volume
        with open(f'/Volumes/{env}/bronze/powerbi/{fil.name}', 'r') as f:
            ## Eliminating empty arrays due to merge issues
            oData = json.loads(f.read().replace('[\n        \n      ]', '""'))['value']
            for r in oData:
                r['GroupID'] = groupId
                if oid is not None:
                    r['ObjectID'] = oid
                
            objects[dType].extend(oData)
    objectsDF = {}
    for k in objects.keys():
        ## Read the JSON data into a dataframe
        objectsDF[k] = spark.read.json(spark.sparkContext.parallelize([json.dumps(objects[k])])) \
            .withColumn('tg_inserted', lit(tstamp)) \
            .withColumn('tg_updated', lit(tstamp))
    ## Delete the files that we read
    for f in dbutils.fs.ls(f'/Volumes/{env}/bronze/powerbi/'):
        dbutils.fs.rm(f.path)
    return objectsDF

# COMMAND ----------

def fixMissingColumns(df, tableName):
    ## If the data we got is missing columns required for the target table, add them as NULL
    target = table(f'bronze.{tableName}')
    for c in [c for c in target.columns if c not in df.columns]:
        print(f'Found {c} not in source table, correcting...')
        df = df.withColumn(c, lit(None))
    return df

# COMMAND ----------

def upsertData (df, tableName, mergeCons):
    ## This function upserts a dataframe into a target table based on the provided merge conditions.
    ## This function also ensures that the DateInserted is not modified on update.
    if spark.catalog.tableExists(f'bronze.{tableName}'):
        ## Print useful info
        print(f'Table {tableName} exists!')
        print(f'Upserting {df.count()} records to {tableName}')

        df = fixMissingColumns(df, tableName)

        ## Prepare the target table
        deltaTable = DeltaTable.forName(spark, f'bronze.{tableName}')
        ## Prepare the merge conditions
        mergeCondition = ' AND '.join([f'targetTable.{col} = update.{col}' for col in mergeCons])
        ## Pick out which columns we want to update (All but DateInserted and PKs)
        updateCols = {x: f'update.{x}' for x in df.columns if x not in mergeCons and x != 'tg_inserted'}

        ## Do the thing
        deltaTable.alias('targetTable') \
            .merge(df.alias('update'), mergeCondition) \
            .whenMatchedUpdate(set = updateCols) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print(f'Table {tableName} doesn\'t exist!')
        df.write.mode('overwrite').format('delta').saveAsTable(f'bronze.{tableName}')

# COMMAND ----------

## Retrieve the groups data
groupResponse = callAPI(token, 'groups')
groupsDF = spark.createDataFrame(json.loads(groupResponse)['value']) \
    .withColumnRenamed('id', 'GroupID') \
    .withColumnRenamed('name', 'GroupName')
display(groupsDF)

# COMMAND ----------

## For each group, pull all of the objects and store their data.
tstamp = datetime.datetime.utcnow().isoformat()
groupCount = groupsDF.select('GroupID').distinct().count()
groupData = groupsDF.repartition(groupCount, 'GroupID') \
        .withColumn('Objects', getObjectsUDF('GroupID')) \
        .withColumn('tg_inserted', lit(tstamp)) \
        .withColumn('tg_updated', lit(tstamp)) \
        .cache()
display(groupData)

# COMMAND ----------

## Process the objects from the previous step
objectsDF = processObjects()

# COMMAND ----------

## For each object, pull the datasources/reports (sub items)
objectData = groupData \
    .select('GroupID', explode('Objects').alias('Object')) \
    .select('GroupID', 'Object.type', 'Object.objectId') \
    .repartition(groupCount, 'GroupID') \
    .withColumn('ObjectData', getObjectDataUDF(col('GroupID'), col('objectid'), col('type'))) \
    .cache()
display(objectData)

# COMMAND ----------

## Process the objects from the previous step
objectDataDF = processObjects()

# COMMAND ----------

display(objectsDF['datasets'])

# COMMAND ----------

## Upsert all of the data to the appropriate table
upsertData(groupData, 'powerbi_groupdata', ['GroupId'])
upsertData(objectsDF['datasets'], 'powerbi_datasets', ['id'])
upsertData(objectsDF['dataflows'], 'powerbi_dataflows', ['objectId'])
upsertData(objectsDF['reports'].distinct(), 'powerbi_reports', ['id'])
upsertData(objectDataDF['datasets'], 'powerbi_datasets_datasources', ['GroupID', 'ObjectID', 'datasourceId', 'gatewayId'])
upsertData(objectDataDF['dataflows'].distinct(), 'powerbi_dataflows_datasources', ['GroupID', 'ObjectID', 'datasourceId', 'gatewayId', 'datasourceType'])
upsertData(objectDataDF['reports'], 'powerbi_reports_pages', ['GroupID', 'ObjectID', 'name'])

# COMMAND ----------

## Printing times to check job speed
print('Start:\t', start)
print('End:\t', datetime.datetime.now())
