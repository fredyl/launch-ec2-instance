fetched_data = response_data[data_key]
            total_pages = int(response_data.get('total_pages', 1))
            print(f"total_pages: {total_pages}")
