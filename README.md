In the current version of Nightly Files, we create a Delta (change) file for vendors of each Mission table each night, and we have history back to 1/1/2022.

The problem with this, is that when we get a NEW vendor, we need to make a full load specifically for that vendor, sometimes multiple times, which is Manual & Time Consuming.

The Objective of this project is to eliminate the need for these full loads.
The way we are going to do that is by creating a full load for each table, automatically on the 1st day of each month.
If we were to complete this objective; When a new vendor comes on, we can tell them were to find the most recent full load, and tell them to ingest the deltas after that.

So what needs doing?
Iterate over each of the "Enabled" views as outlined in the Control Table.
Control Table "{env}.admin.vendornightlyfiles".
For each of the files, produce an output file for each Year/Month combo.
This can be managed for you by utilizing partitions.
The format of the output should match the data found in the current daily deltas.
Files for today can be found in "/mnt/edhingest2/zonea/missiontables/2024/11/12/{filename}.txt" to be used for comparison.
Rename each output file to reflect the contents of the data.
Example: "slsah_202411.txt" for the SLSAH file with data most recently updated in 11/2024 (Based on the XXDATE).
Put each output file into a folder so all related data is held together.
Example: "/Volumes/dev/silver/MonthlyFullLoads/slsah/slsah_202411.txt" (Relevant portion bolded & italicized).
Make the output location CONDITIONAL based on the branch. Dev & UAT Land in Volumes, Prod lands in the Production ADLS location.
Production location TBD By Blake.
Do not land test data in Production.
​Make the system capable of purging the old Full Loads before loading the new full loads.
Do NOT test this in production without full code review & sign off.
Purge old deltas based on Retention period TBD by Shane.
Do NOT test this in production without full code review & sign off.
