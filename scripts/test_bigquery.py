from google.cloud import bigquery
from db_manager import store_conflict_data

# Initialize BigQuery Client
client = bigquery.Client()

# Define Query (Fixing SQLDATE Data Type)
query = """
SELECT SQLDATE, Actor1Name, Actor2Name, EventCode, GoldsteinScale, AvgTone, ActionGeo_FullName
FROM `gdelt-bq.gdeltv2.events`
WHERE CAST(SQLDATE AS STRING) >= FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY))
  AND EventRootCode BETWEEN '14' AND '19'
ORDER BY SQLDATE DESC
LIMIT 100;
"""

# Run Query
query_job = client.query(query)
results = query_job.result()

# Process Results
conflict_data = []
for row in results:
    conflict_data.append({
        "date": row.SQLDATE,
        "actor_1": row.Actor1Name,
        "actor_2": row.Actor2Name,
        "event_code": row.EventCode,
        "goldstein_scale": row.GoldsteinScale,
        "sentiment": row.AvgTone,
        "location": row.ActionGeo_FullName
    })

# Store in MongoDB
store_conflict_data(conflict_data)
print("✅ Conflict data successfully stored in MongoDB.")
