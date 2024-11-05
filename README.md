dbutils.fs.mkdirs('/Volumes/{env}/bronze_vendor/holmanP/{data_type}')


def fetch_data_from_url(url, page, data_type, code_value):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    filePath = f'{chk_path}{data_type}_{code_value}_{page}.json'
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
    # with open(chk_path, 'w') as f:
    #     f.write(response.text)

    with open(filePath, 'w') as f:
        f.write(response.text)
    
    return filePath


https://github.com/user-attachments/assets/6bbc53c6-224c-4822-8c4d-d79f197e4343

