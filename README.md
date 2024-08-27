An error occurred: 
[PARSE_SYNTAX_ERROR] Syntax error at or near '['. SQLSTATE: 42601 (line 4, pos 22)

== SQL ==

            MERGE INTO dev.bronze.pbi_reports AS target
            USING temp_view AS source
            ON target.['id', 'datasetId', 'name'] = source.['id', 'datasetId', 'name']
----------------------^^^
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
