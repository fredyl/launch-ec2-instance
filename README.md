from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Configuration variables
control_table = "prod.admin.vendornightlyfiles"
source_table = "prod.gold_vendor_nightly"
yearmonth = datetime.now().strftime("%Y%m")
prev_month = (datetime.now() - relativedelta(months=1)).strftime("%Y%m")
prev_month_2 = (datetime.now() - relativedelta(months=2)).strftime("%Y%m")
backup_path = f"/Volumes/dev/bronze_vendor/nightlyfiles_{prev_month}_backup"
base_path = f"/Volumes/dev/bronze_vendor/nightlyfiles"

prev_volume = f"nightlyfiles_{prev_month}_backup"
prev_volume_2 = f"nightlyfiles_{prev_month_2}_backup"
main_volume = "nightlyfiles"

# Get volume names
def get_volumes():
    return [row["volume_name"] for row in spark.sql(f"SHOW VOLUMES IN {env}.bronze_vendor").select("volume_name").collect()]

# Manage volumes in parallel
def manage_volumes():
    volume_names = get_volumes()
    with ThreadPoolExecutor() as executor:
        if prev_volume_2 in volume_names:
            executor.submit(
                spark.sql, f"DROP VOLUME IF EXISTS {env}.bronze_vendor.{prev_volume_2}"
            )

        if prev_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.bronze_vendor.{prev_volume} RENAME TO {env}.bronze_vendor.{prev_volume_2}"
            )

        if main_volume in volume_names:
            executor.submit(
                spark.sql, f"ALTER VOLUME {env}.bronze_vendor.{main_volume} RENAME TO {env}.bronze_vendor.{prev_volume}"
            )

    volume_names = get_volumes()
    if main_volume not in volume_names:
        spark.sql(f"CREATE VOLUME IF NOT EXISTS {env}.bronze_vendor.nightlyfiles")

# Fetch enabled views
def get_enabled_views(control_table):
    return spark.table(control_table).filter("enabled = true").select("sourceTable").rdd.flatMap(lambda x: x).collect()

# Generate file paths
def generate_file_path(view_names):
    return [
        f"{backup_path}/{view.split('.')[-1]}/{view.split('.')[-1]}_{yearmonth}.txt"
        for view in view_names
    ]

# Create backup files in parallel
def create_backup_file(file_paths):
    backup_dirs = set("/".join(fp.replace(".txt", "_backup.txt").split("/")[:-1]) for fp in file_paths)
    for backup_dir in backup_dirs:
        dbutils.fs.mkdirs(backup_dir)

    def backup_file(file_path):
        backup_path = file_path.replace(".txt", "_backup.txt")
        dbutils.fs.cp(file_path, backup_path, recurse=True)
        print(f"Backup created at: {backup_path}")

    with ThreadPoolExecutor() as executor:
        executor.map(backup_file, file_paths)

# Fetch vendor data in parallel
def fetch_vendor_data(view_names, base_path, source_table):
    dbutils.fs.mkdirs(base_path)

    def process_view(view):
        view_name = view.split(".")[-1]
        data_df = spark.read.format("delta").table(f"{source_table}.{view_name}")
        data_df = data_df.select([data_df[col].cast("string").alias(col) for col in data_df.columns])

        file_path_txt = f"{base_path}/{view_name}/{view_name}_{yearmonth}.txt"
        data_df.repartition(50).write.mode("overwrite").format("csv") \
            .option("header", "true") \
            .option("delimiter", "|") \
            .save(file_path_txt)
        print(f"Saved data to {file_path_txt}")

    with ThreadPoolExecutor() as executor:
        executor.map(process_view, view_names)

# Main execution
if __name__ == "__main__":
    manage_volumes()

    enabled_views = get_enabled_views(control_table)
    file_paths = generate_file_path(enabled_views)

    if main_volume in get_volumes():
        create_backup_file(file_paths)
    else:
        print("No backup required")

    fetch_vendor_data(enabled_views, base_path, source_table)
