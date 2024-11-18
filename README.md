Day 1: Requirement Analysis and Control Table Integration

Steps:

Review project requirements.
Identify:
Control table structure.
Data format and retention logic.
File naming convention and target environments.
Plan directory structure and partition schema.
Query the control table:
Connect to {env}.admin.vendornightlyfiles.
Fetch all rows where enabled = True.
Validate data availability and structure.
Test sample queries for data integrity.
Pseudo Code:


Connect to control_table = "{env}.admin.vendornightlyfiles"
Fetch enabled_views = Query("SELECT view_name FROM control_table WHERE enabled = True")
Validate enabled_views for structure and integrity
Plan directory structure based on:
    - /Volumes/{env}/silver/MonthlyFullLoads/{viewname}/{Year}/{Month}/{viewname}.txt
Output: enabled_views list


Day 2-3: Data Query, Partition Logic, File Naming, and Directory Creation

Steps:

Query data for XXDATE >= 20220101.
Extract Year and Month from XXDATE.
Partition data dynamically by Year and Month.
Define file naming convention:
Format: {viewna[pseudo.docx](https://github.com/user-attachments/files/17804768/pseudo.docx)
me}/{Year}/{Month}/{viewname}.txt.
Create directories dynamically based on the environment.
Pseudo Code:

For each view in enabled_views:
    data = Query(f"SELECT * FROM {view} WHERE XXDATE >= 20220101")
    Add columns:
        data["Year"] = Extract Year from XXDATE
        data["Month"] = Extract Month from XXDATE
    partitions = data.groupby("Year", "Month")
    For each (Year, Month) in partitions:
        directory = f"/Volumes/{env}/silver/MonthlyFullLoads/{view}/{Year}/{Month}"
        Create directory if not exists
        filename = f"{directory}/{view}.txt"
Output: Script for partition logic and directory creation


Day 4-5: Output File Writing and Environment-Specific Outputs

Steps:

Write partitioned data to text files in the directory.
Validate file contents and naming convention.
Parameterize script for multiple environments (Dev, UAT, Prod).
Test output files in Dev.
Pseudo Code:

For each (Year, Month) in partitions:
    directory = f"/Volumes/{env}/silver/MonthlyFullLoads/{view}/{Year}/{Month}"
    Create directory if not exists
    filename = f"{directory}/{view}.txt"
    Write data to filename
    If environment == "Prod":
        Adjust path to "/mnt/prod/silver/MonthlyFullLoads"
Output: Script to write partitioned data to environment-specific paths


Day 6-7: Vendor Permissions, Security, and Retention Logic

Steps:

Assign Azure Storage permissions:
Restrict access to specific folders for vendors.
Develop retention logic:
Identify files older than the retention period.
Purge old files from the directory.
Pseudo Code:


Assign Azure Storage permissions:
    For each vendor:
        Assign read access to specific directories

For each directory in /Volumes/{env}/silver/MonthlyFullLoads/:
    Check all files
    If file_date < Retention_Threshold:
        Delete file
Output: Permission assignments and retention logic script


Day 8: Scheduling, Final Testing, and Deployment

Steps:

Research scheduling tools (e.g., cron, Databricks Jobs, or ADF).
Integrate script with the scheduling tool.
Perform end-to-end testing in Dev and UAT:
Validate outputs, retention logic, permissions, and error handling.
Pseudo Code:

Schedule job:
    If using cron:
        Schedule: "0 0 1 * * python /path/to/script.py"
    If using Databricks Jobs:
        Create monthly trigger

Run script in Dev and UAT:
    Validate:
        - Files in correct structure and location
        - Retention logic correctly purges old files
        - Permissions allow vendor access
        - No errors in logs
Output: Fully tested and scheduled script

[pseudo.docx](https://github.com/user-attachments/files/17804769/pseudo.docx)
