control_table = "prod.admin.vendornightlyfiles"
source_table = "prod.gold_vendor_nightly"

# Get enabled views
enabled_views = spark.sql(f"SELECT sourceTable FROM {control_table} WHERE enabled = true").collect()

# Generate SQL for each view
queries = []
for row in enabled_views:
    view_name = row["sourceTable"].split(".")[-1]
    query = f"""
        SELECT 
            '{view_name}' AS view_name,
            COUNT(*) AS row_count
        FROM 
            {source_table}.{view_name}
    """
    queries.append(query)

# Combine queries with UNION ALL
final_query = " UNION ALL ".join(queries)
sorted_query = f"""
    SELECT * FROM ({final_query}) AS all_views
    ORDER BY row_count ASC
"""

# Run the query
result = spark.sql(sorted_query)
result.show()
