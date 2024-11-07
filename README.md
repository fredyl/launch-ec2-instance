SELECT SUM(duplicate_count) AS total_duplicates
FROM (
    SELECT COUNT(*) - 1 AS duplicate_count
    FROM your_table_name
    GROUP BY column1
    HAVING COUNT(*) > 1
) AS duplicates;
