# Databricks notebook source
# Define the Databricks workspace URL
workspace_url = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiUrl()
    .getOrElse(None)
)

# Define the Databricks access token
access_token = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .apiToken()
    .getOrElse(None)
)

# Define the headers for the API request
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json",
}

# Define the list of user emails to be created
users = [
    "first.last@company.com",
]

# COMMAND ----------

import requests
import json
from time import sleep

# Create users
for user in users:
    response = requests.post(
        f"{workspace_url}/api/2.0/preview/scim/v2/Users",
        headers=headers,
        data=json.dumps({"userName": user}),
    )

    if response.status_code == 201:
        print(f"User {user} created successfully.")
    else:
        if "already exists" not in response.text:
            print(f"Error creating user {user}: {response.text}")

    sleep(2)  # Sleep for 2 seconds
