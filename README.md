def generate_paginated_urls(token, data_type, code_key, code_value, total_pages, batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = [
        {"pageNumber": page, "url": f"{base_url}{data_type}?{code_key}={code_value}&page={page}&pageSize={batch_size}"}
        for page in range(1, total_pages + 1)
    ]
    return pagination_urls, len(pagination_urls)
