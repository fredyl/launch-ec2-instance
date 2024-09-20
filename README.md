import datetime
import json
import requests
from pyspark.sql.functions import col, lit, when

"""
Checking if the table exists
"""
def table_exists(table_name):
    try:
        spark.table(table_name)  # Changed 'table' to 'spark.table'
        return True
    except:
        return False


"""
Optimized Date Loop function
"""
def getDateLoop(start, end):
    days = (end.date() - start.date()).days + 1
    date_list = []
    
    for d in range(days):
        date = start + datetime.timedelta(days=d)
        if days == 1:
            date_list.append((start.strftime("'%Y-%m-%dT%H:%M:%SZ'"), end.strftime("'%Y-%m-%dT%H:%M:%SZ'")))
        elif d == days - 1:
            date_list.append((date.strftime("'%Y-%m-%dT00:00:00Z'"), end.strftime("'%Y-%m-%dT%H:%M:%SZ'")))
        elif d == 0:
            date_list.append((start.strftime("'%Y-%m-%dT%H:%M:%SZ'"), date.strftime("'%Y-%m-%dT23:59:59Z'")))
        else:
            date_list.append((date.strftime("'%Y-%m-%dT00:00:00Z'"), date.strftime("'%Y-%m-%dT23:59:59Z'")))
    
    return date_list


"""
Optimized getResultForQuery function
- Batching results and writing at the end to reduce I/O operations
"""
def getResultForQuery(token, start=None, end=None, ct=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    results = []  # Collect results for batch writing
    while True:
        if ct is None:
            url = f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime={start}&endDateTime={end}'
        else:
            url = f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?continuationToken={ct}'
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Error: {response.status_code}, {response.text}")
            break

        responseJSON = response.json()
        results.append(responseJSON['activityEventEntities'])
        
        if 'continuationToken' not in responseJSON:
            break
        ct = responseJSON['continuationToken']

    # Writing results in bulk to a single JSON file
    startStr = start.replace("'", "").split('T')[0]
    volume_path = f'dbfs:/Volumes/{env}/bronze/powerbi/PBIAdminEvents_{startStr}.json'
    dbutils.fs.put(volume_path, json.dumps(results), overwrite=True)
    
    return (responseJSON.get('lastResultSet', False), responseJSON.get('continuationToken', None))


"""
Checking if the table exists and setting startDateTime
"""
tableExists = table_exists('bronze.pbi_activity_event')

endDateTime = datetime.datetime.now()

if not tableExists:
    startDateTime = endDateTime - datetime.timedelta(days=29)
else:
    # Optimized aggregation to fetch the max CreationTime
    startDateTime = spark.table('bronze.pbi_activity_event').select(col('CreationTime').cast(TimestampType())).agg({'CreationTime': 'max'}).first()[0]

dateLoop = getDateLoop(startDateTime, endDateTime)

for (start, end) in dateLoop:
    print(start, end)
    lastResultSet, continuationToken = getResultForQuery(access_token, start=start, end=end)
    loopNumber = 1
    while lastResultSet == False:
        lastResultSet, continuationToken = getResultForQuery(access_token, start=start, ct=continuationToken)
        loopNumber += 1

"""
Creating a DataFrame from the JSON data and saving it to Delta table
"""
timestamp = datetime.datetime.utcnow().isoformat()

# Optimized reading JSON files into DataFrame with repartitioning
file_path = f'/Volumes/{env}/bronze/powerbi/*.json'
df = spark.read.json(file_path).repartition(10)  # Repartition to improve performance

# Adding timestamp columns
df = df.withColumn('LastModified', lit(timestamp))
df = df.withColumn('insertTime', lit(timestamp))

# Dropping 'SharingInformation' if empty
df = df.withColumn('SharingInformation', when(col('SharingInformation') == lit([]), None).otherwise(col('SharingInformation')))
if df.filter(col('SharingInformation').isNotNull()).count() == 0:
    df = df.drop('SharingInformation')

# Optimized Delta Table merge logic
if not tableExists:
    print('Table Doesnt Exist')
    df.write.mode('overwrite').format('delta').saveAsTable('bronze.pbi_activity_event')
else:
    print('Table Exists')
    
    # Use Delta Merge instead of collecting IDs for deletion
    df.alias('new').merge(
        spark.table('bronze.pbi_activity_event').alias('existing'),
        'new.ID = existing.ID'
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

"""
Clean up JSON files from DBFS after processing
"""
for f in dbutils.fs.ls(f'/Volumes/{env}/bronze/powerbi/*.json'):
    dbutils.fs.rm(f.path)
