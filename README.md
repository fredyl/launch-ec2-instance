




################################################################################################
# Power Bi API call to pull data for power bi activity event create a table and load the data. # 
################################################################################################

url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

headers = {
    "Content-Type": "application/x-www-form-urlencoded"
}

body = f'client_id={client_id}&grant_type=client_credentials&scope=https%3A%2F%2Fanalysis.windows.net%2Fpowerbi%2Fapi%2F.default&client_secret={client_secret}'

response = requests.get(url, headers=headers, data=body)

token = json.loads(response.text)['access_token']





#############################################################################
"""
checking if table exists
"""

def table_exists(table_name):
    try:
        table(table_name)
        return True
    except:
        return False
###########################################################################

def getDateLoop(start, end):

    """
    the function loops and generates a list of date ranges between start and end datetime
    in ISO format
    """

    # For each day between start and end, yield a string in 'yyyy-mm-ddThh:mm:ssZ' format
    days = (end.date() - start.date()).days+1
    

    dateList = []

    for d in range(0, days):
        date = start+datetime.timedelta(days=d)
        if days == 1:
            dateList.append((start.strftime("'%Y-%m-%dT%H:%M:%SZ'"), end.strftime("'%Y-%m-%dT%H:%M:%SZ'")))
        elif d == days-1:
            dateList.append((date.strftime("'%Y-%m-%dT00:00:00Z'"),end.strftime("'%Y-%m-%dT%H:%M:%SZ'")))
        elif d == 0:
            dateList.append((start.strftime("'%Y-%m-%dT%H:%M:%SZ'"), date.strftime("'%Y-%m-%dT23:59:59Z'")))
        else:
            dateList.append((date.strftime("'%Y-%m-%dT00:00:00Z'"), date.strftime("'%Y-%m-%dT23:59:59Z'")))
    
    return dateList
    
########################################################################

def getResultForQuery(token, start=None, end=None, ct=None, ln=None):
    """
    Function interacts with activity events API and save the 
    results to a json file in Databricks file system(DBFS)
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    if ct is None:
        url = f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime={start}&endDateTime={end}'
    else:
        url= f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?continuationToken=\'{ct}\''
    
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
        return (None, None)

    responseJSON = json.loads(response.text)
    lastResultSet = responseJSON['lastResultSet']
    continuationToken = responseJSON['continuationToken']
    entities = responseJSON['activityEventEntities']
    startStr = start.replace("'", "").split('T')[0]

    # testOut = '{"entities": '+json.dumps(entities)+'}'

    volume_path = f'dbfs:/Volumes/dev/bronze/powerbi/PBIAdminEvents_{startStr}_{ln or "0"}.json'
    dbutils.fs.put(volume_path, json.dumps(entities), overwrite=True)

    return (lastResultSet, continuationToken)
    ###################################################

"""
checking if table exist and setting a staerDateTime
"""

tableExists = table_exists('dev.bronze.pbi_activity_event')

endDateTime = datetime.datetime.now()

if not tableExists:
    startDateTime = endDateTime - datetime.timedelta(days=29)
else:
    startDateTime = table('dev.bronze.pbi_activity_event').select(col('CreationTime').cast(TimestampType())).agg({'CreationTime': 'max'}).first()[0]

######################################    
dateLoop = getDateLoop(startDateTime, endDateTime)
for (start, end) in dateLoop:
    print(start,end)
    lastResultSet = None
    continuationToken = None
    lastResultSet, continuationToken = getResultForQuery(token, start=start, end=end)
    loopNumber = 1
    while lastResultSet == False:
        lastResultSet, continuationToken = getResultForQuery(token, start=start, ct=continuationToken, ln=loopNumber)
        loopNumber += 1
        
"""
Creating a dataframe from the json data and saving it to delta table
"""

df = spark.read.json('/Volumes/dev/bronze/powerbi/*.json')
df = df.withColumn('SharingInformation', when(col('SharingInformation') == lit([]), None).otherwise(col('SharingInformation')))
if df.filter(col('SharingInformation').isNotNull()).count() == 0:
    df = df.drop('SharingInformation')

if tableExists != True:
    print('Table Doesnt Exist')
    df.write.mode('overwrite').format('delta').saveAsTable('dev.bronze.pbi_activity_event')
else:
    print('Table Exists')
    rmove = df.join(table('dev.bronze.pbi_activity_event'), 'ID', 'inner').select('ID').collect()
    ids = []
    for r in rmove:
        ids.append("'"+r[0]+"'")
    print(len(ids))
    if len(ids) > 0:
        spark.sql(f'DELETE FROM dev.bronze.pbi_activity_event WHERE ID IN ({",".join(ids)})')
    df.write.mode('append').format('delta').option('mergeSchema', True).saveAsTable('dev.bronze.pbi_activity_event')


for f in dbutils.fs.ls('/Volumes/dev/admin/powerbi/'):
    dbutils.fs.rm(f.path)
