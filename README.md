limit = 50
all_vehicles=[]
page =1
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/video/vehicles?PageNumber={page}&PageSize={limit}"

all_vehicles = get_lytx_paging_data(endpoint, page, limit)

print(len(all_vehicles))


UnboundLocalError: cannot access local variable 'page_endpoint' where it is not associated with a value
