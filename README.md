def create_backUp_file(file_paths):
    backup_path = f'/Volumes/dev/bronze_vendor/nightlyfiles_backup'
    for file_path in file_paths:
        backup_path = file_path.replace(".csv", "_backup.csv")
        # Ensure the backup directory exists
        backup_dir = "/".join(backup_path.split("/")[:-1])
        dbutils.fs.mkdirs(backup_dir)
        # Copy the file to the backup location
        dbutils.fs.mv(file_path, backup_path, recurse=True)
        print(f"Copied {file_path} to {backup_path}")

# Example usage
create_backUp_file(file_paths)


sql_spark= f"ALTER VOLUME  {env}.bronze_vendor.nightlyfiles RENAME TO {env}.bronze_vendor.nightlyfiles_backup"
spark.sql(sql_spark)


yearmonth = datetime.now().strftime("%Y%m")
