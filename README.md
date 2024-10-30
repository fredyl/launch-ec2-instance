what is the difference between these two codes

 base_url = "https://customer-experience-api.arifleet.com/v1/"
    total_pages = get_all_pages(token, data_type, code_key, code_value)
    if total_pages == 0:
        raise Exception("No pages found")

    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    print(f" loops : {loops}")

    for l in range(loops):
        page = l + 1
        url = f"{base_url}{data_type}?{code_key}={code_value}"
        pagination_urls.append({"pageNumber": page, "url": url})
        # print(pagination_urls)

    return pagination_urls, loops

def generate_paginated_urls(data_key, code_key, code_value,total_pages,batch_size=200):
    base_url = "https://customer-experience-api.arifleet.com/v1/"
    pagination_urls = []
    loops = (total_pages // batch_size) + 1
    print(f" loops : {loops}")
    for l in range(loops):
        for page in range(l * batch_size + 1, min((l + 1) * batch_size + 1, total_pages +1)):
            url = f"{base_url}{data_type}?{code_key}={code_value}&pageNumber={page}"
            pagination_urls.append({"pageNumber": page, "url": url})
    return pagination_urls, loops
