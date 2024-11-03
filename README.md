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


paginations_url = generate_paginated_urls('violation','violations', 'violationDateCode',1, total_pages)
{"data_type": "violation", "code_key": "violationDateCode", "data_key": "violations", "primary_key": "record_id"},
print(paginations_url)


 num_batches : 1
len 4
([{'pageNumber': 1, 'url': 'https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=1&pageNumber=1'}, {'pageNumber': 2, 'url': 'https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=1&pageNumber=2'}, {'pageNumber': 3, 'url': 'https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=1&pageNumber=3'}, {'pageNumber': 4, 'url': 'https://customer-experience-api.arifleet.com/v1/violation?violationDateCode=1&pageNumber=4'}], 1)
