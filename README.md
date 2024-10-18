def get_last_checkpoint(endpoint_key, default=0):
    global global_checkpoint
    return global_checkpoint if global_checkpoint != 0 else default


global_checkpoint = {}
def save_checkpoint(endpoint_key, page_number):
    # dbutils.fs.put("dbfs:/Volumes/{env}/bronze/holman_last_page.txt", str(page_number), overwrite=True)
    # print(f"File Created: /Volumes/{env}/bronze/holman_last_page.txt")
    global global_checkpoint
    global_checkpoint = page_number
    print(f"Checkpoint Updated to {page_number}")
