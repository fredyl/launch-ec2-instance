why such a result in pagination

getting data for page:1
getting data for page:2
getting data for page:3
getting data for page:4
getting data for page:1


while True:
    # endpoint = f"/video/safety/eventsWithMetadata?dateOption=recordDate&sortDirection=desc&sortBy=recordDate&includeSubgroups=true&limit={limit}&page={page}"
    endpoint =f"/video/safety/eventsWithMetadata?from={from_date}&to={to_date}&dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
    response_data = lytx_get_repoonse_from_event_api(endpoint)
    if not response_data:
        print("No more data")
        break
    # print(response_data)
    print(f"getting data for page:{page}")
    all_events_metadata.extend(response_data)
    if len(response_data) < limit:
    # if not response_data:
        break
    page += 1
