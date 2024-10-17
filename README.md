def save_checkpoint(page_number):
    with open("/dbfs/tmp/last_page.txt", "w") as file:
        file.write(str(page_number))

def get_last_checkpoint(default=0):
    try:
        with open("/dbfs/tmp/last_page.txt", "r") as file:
            return int(file.read())
    except FileNotFoundError:
        return default
