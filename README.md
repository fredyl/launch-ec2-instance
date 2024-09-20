def getResultForQuery(token, start=None, end=None, ct=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    
    if ct is None:
        url = f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?startDateTime={start}&endDateTime={end}'
    else:
        # Pass the continuation token exactly as it is, without quotes
        url = f'https://api.powerbi.com/v1.0/myorg/admin/activityevents?continuationToken={ct}'
    
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Error: {response.status_code}, {response.text}")
        return (None, None)

    responseJSON = response.json()
    lastResultSet = responseJSON.get('lastResultSet', False)
    continuationToken = responseJSON.get('continuationToken', None)
    
    # Save the response JSON to DBFS
    startStr = start.replace("'", "").split('T')[0]  # Format start date
    volume_path = f'dbfs:/Volumes/{env}/bronze/powerbi/PBIAdminEvents_{startStr}_{loopNumber or "0"}.json'
    dbutils.fs.put(volume_path, json.dumps(responseJSON['activityEventEntities']), overwrite=True)

    return (lastResultSet, continuationToken)
