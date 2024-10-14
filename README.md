all_vehicles=[]
limit = 100
page =1 
table_name = f"bronze.lytx_video_vehicles_vehicleId"
endpoint = f"/vehicles/all?limit={limit}&page={page}&includeSubgroups=true"

all_vechicles = get_lytx_paging_data(endpoint,page)
print(len(all_vehicles))
