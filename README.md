# Deduplicate by primary key
                    unique_data = [item for item in parsed_data if item[primary_key] not in seen_ids]
                    data_list.extend(unique_data)
                    # Track seen primary keys
                    seen_ids.update(item[primary_key] f
