

def fetch_data_from_url(url, page, data_type, code_value, d_path,key_value=None):
    '''
    Fetch data from given urland writing to file in file path in json format''
    '''
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if data_type == "billing":
        filePath = f'{d_path}/{data_type}_{code_value}_{key_value}_{page}.json'
    else:
        filePath = f'{d_path}/{data_type}_{code_value}_{page}.json' # file path
    if response.status_code != 200:
        return f"Error: {response.status_code}"

    #writting response to file
    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath


fetch_data_udf = udf(fetch_data_from_url, StringType())
