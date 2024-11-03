def fetch_data_from_url(url):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f"Bearer {token}"
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        return f"Error: {response.status_code}"
    
        chk_path = '/Volumes/{env}/bronze_vendor/holman'
        with open(chk_path, 'w') as f:
            f.write(response.text)
        return chk_path
    
check_path = fetch_data_from_url('https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=1&pageNumber=1')
print(check_path)
