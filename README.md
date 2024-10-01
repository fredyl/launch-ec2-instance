 fetch_events_meta_data():
    limit = 1000
    all_events_metadata = []
    page = 1
    while True:
        endpoint = f"/video/safety/eventsWithMetadata?dateOption=lastUpdatedDate&sortDirection=desc&sortBy=lastUpdatedDate&includeSubgroups=true&limit={limit}&page={page}"
        response_data = lytx_get_repoonse_from_event_api(endpoint)
        # print(response_data)
        print(f"getting data for page:{page}")
        all_events_metadata.extend(response_data)
        if len(response_data) < limit:
            break
        page += 1

        return all_events_metadata
meta_data = fetch_events_meta_data()
