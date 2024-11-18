Here's a step-by-step time frame over to complete the project within the next 2 weeks:

Day 1: Requirement Analysis and Planning and Control Table Integration

Task:
Review project requirements in detail.
Identify key components: control table, data format, file naming convention, retention logic, and partitioning.
Define target environments 
Plan the directory structure and partitioning schema.
Query the control table to fetch enabled views.
Validate the structure and availability of required data.
Test sample queries to ensure data integrity.
Deliverable:
A detailed project plan and directory structure template.
Function/script to retrieve enabled views from the control table.

Day 2 to 3: Data Query and Partition Logic and File Naming and Directory Creation
Task:
Develop logic to query data for XXDATE >= 20220101.
Extract and validate Year and Month from XXDATE.
Partition data dynamically by Year and Month.
Define the file naming convention (viewname/year/month/viewname.txt).
Develop logic to create directories dynamically based on the environment.
Test directory creation in Dev environments.
Deliverable:
Script to create directory structure and generate partitioned filenames.
A script to fetch and partition data by Year and Month.

Day 4 to 5: Output File Writing and Environment-Specific Outputs

Task:
Write partitioned data to text files in the correct directory.
Ensure output matches the daily delta format.
Validate the file contents and naming convention.
Parameterize the script to handle any environments.
Ensure Dev/UAT files are written to /Volumes.
Test outputs in Dev 
Deliverable:
Script to write partitioned data to text files.
Fully functional script handling environment-specific paths.

Day 6 and 7 : Vendor Permissions and Security and Retention and Purging Logic
Task:
Assign access roles for Dev/UAT/Prod folders.
Which will have to find out Trugreens steps and procedures to assign permission to a vendor
Ensure data access is restricted to necessary folders.
Develop retention logic to delete files older than the defined period.
Test purging on sample directories and files.
Verify that no unintended data is deleted.
Deliverable:
Retention policy script integrated into the main process.
Documented steps and tested permissions for vendor access.

From Day 8: Final Testing and Deployment
Task:
Find out scheduling tools to be used for the monthly run(e.g cron, databricks job etc)
Integrate script with scheduling tool
Perform end-to-end testing in Dev environments.
Validate outputs, retention, permissions, and error handling.
Testing in UAT.
Deliverable:
Complete, tested script ready for production deployment.
Documentation of the process and configuration.

Proposed Step-by-Step Solution
1. Fetch Enabled Views
Connect to the control table {env}.admin.vendornightlyfiles.
Query for all rows where enabled = True.
2. Generate Full Loads : which can also be archived through parallel processing
For each enabled view create a partition
First load, load all data of the views from historic data source
Then write a code to run every 1st of the month to Query historical data to the last day of the previous month.
Use Year and Month columns for partitioning.
Ensured files are written to /Volume/{env}/viewname/year/month/viewname.txt.
Example: slsah/2024/11/slsah.txt.
Format filenames to (/volume/viewname/viewname_YYYYMM.txt)
Processing XXDATE:
Extracted Year and Month using integer arithmetic.
Year: XXDATE
Month: XXDATE 
3. Write to Target Directories
Based on the environment (Dev):
Dev: Write to /Volumes/{env}/silver/MonthlyFullLoads/{viewname}/{ viewname _YYYYMM}.txt.
Prod: Write to /mnt/prod/silver/MonthlyFullLoads/{tablename}/{tablename_YYYYMM}.txt.
4. Purge Old Data
Retention Logic:
Dynamically checks year/month directories to ensure files older than the retention period are purged.
Retrieve and apply retention periods.
Delete files older than the retention threshold.
5. Parameterize Scheduling
6: Granting Vendor Permissions
For the respective location:
Assign Azure Storage permissions for specific folders under /Volumes/{env}/silver/MonthlyFullLoads/.




