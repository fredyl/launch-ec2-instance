import os

def rename_files_to_backup(file_paths):
    '''
    Rename files in the given file paths to their backup versions.
    Args:
        file_paths (list): List of file paths to be renamed.

    Returns:
        None: Files are renamed in place.
    '''
    for file_path in file_paths:
        # Split the file path into directory and file name
        directory, file_name = os.path.split(file_path)

        # Modify the file name to append '_backup' before the extension
        file_name_backup = file_name.replace(".csv", "_backup.csv")

        # Create the full path for the renamed file
        backup_file_path = os.path.join(directory, file_name_backup)

        try:
            # Rename the file to the backup version
            os.rename(file_path, backup_file_path)
            print(f"Renamed: {file_path} -> {backup_file_path}")
        except FileNotFoundError:
            print(f"File not found: {file_path}")
