# Databricks notebook source
# Define the Databricks workspace URL and token
workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)

access_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

# Define the headers
headers = {
    "Authorization": f"Bearer {access_token}",
}

# Get the current metastore ID
metastore_id = spark.sql("SELECT current_metastore() as metastore_id").collect()[0][
    "metastore_id"
]
metastore_id = metastore_id[metastore_id.rfind(":") + 1 :]

# COMMAND ----------

import requests
from time import sleep

# Fetch the list of system schemas from the Unity Catalog
response = requests.get(
    f"{workspace_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas",
    headers=headers,
)

# Check if the request was successful
if response.status_code == 200:
    schemas_to_enable = []
    schemas_already_enabled = []
    schemas_unavailable = []

    # Categorize schemas based on their state
    for schema in response.json()["schemas"]:
        if schema["state"].lower() == "available":
            schemas_to_enable.append(schema["schema"])
        elif schema["state"].upper() == "ENABLE_COMPLETED":
            schemas_already_enabled.append(schema["schema"])
        else:
            schemas_unavailable.append(schema["schema"])

    # Print the categorized schemas
    print(f"Schemas to enable: {schemas_to_enable}")
    print(f"Schemas already enabled: {schemas_already_enabled}")
    print(f"Schemas unavailable: {schemas_unavailable}")
else:
    raise Exception(response.text)

# COMMAND ----------

# Enable each schema in the schemas_to_enable list
for schema in schemas_to_enable:
    # Send a PUT request to enable the schema
    response = requests.put(
        f"{workspace_url}/api/2.0/unity-catalog/metastores/{metastore_id}/systemschemas/{schema}",
        headers=headers,
    )

    # Check if the request was successful
    if response.status_code == 200:
        print(f"Schema {schema} enabled successfully.")
    else:
        print(response.text)

    # Wait for 2 seconds before the next request
    sleep(2)
