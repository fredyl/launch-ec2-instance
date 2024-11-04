data_list = []
for row in cleaned_data.collect():
    # Debugging prints
    print(f"Processing row: {row}")
    print(f"Value of row[data_key]: {row[data_key] if data_key in row else 'data_key not in row'}")

    # Check if the data_key exists in the row and is a string
    if data_key in row and isinstance(row[data_key], str):
        if not row[data_key].startswith("Error") and not row[data_key].startswith("Exception"):
            try:
                parsed_data = json.loads(row[data_key])
                print(f"Parsed data: {parsed_data}")  # Debug print

                # If parsed_data is a list, extend it into data_list
                data_list.extend(parsed_data)
                
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON for row: {row[data_key]} - {e}")
                
print("Final data_list contents:")
print(json.dumps(data_list, indent=2))
