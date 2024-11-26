# Databricks notebook source
# DBTITLE 1,Creating a Table with Structured JSON Content
# MAGIC %sql
# MAGIC create
# MAGIC or replace table catalog.default.json_content as
# MAGIC select
# MAGIC   '{
# MAGIC     "data": {
# MAGIC       "content": {
# MAGIC         "007c0639-6ab8-44f6-8121-5f4db3802f2c": {
# MAGIC           "key_1": "key value 1",
# MAGIC           "key_2": "key value 2"
# MAGIC         },
# MAGIC         "86caa1b2-d641-4c34-aa5b-4e2ab1cf2f13": [
# MAGIC           "array value 1",
# MAGIC           "array value 2"
# MAGIC         ],
# MAGIC         "d07ad0f6-989b-4092-bce9-aaae819cce48": "string value"
# MAGIC       }
# MAGIC     },
# MAGIC     "service": "payment"
# MAGIC   }' as json_data

# COMMAND ----------

# DBTITLE 1,Loading Data from Spark Table into DataFrame
df = spark.table(f"catalog.default.json_content")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate schema and parse data

# COMMAND ----------

# DBTITLE 1,Parsing and Displaying JSON Data in a DataFrame
schema = df.selectExpr("schema_of_json_agg(json_data) as schema").first()[0]
print(schema)

# COMMAND ----------

from pyspark.sql.functions import col, from_json

# Parse the JSON data in the "json_data" column using the new schema
parsed_df = df.withColumn("json_data", from_json(col("json_data"), schema))

# Display the resulting DataFrame
display(parsed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Generate and modify schema and parse data

# COMMAND ----------

# DBTITLE 1,Updating Nested Spark DataFrame Field Data Types
from pyspark.sql.types import StructType, StructField, DataType

# Function to replace the data type of a specified field in a schema
def replace_field_type(schema: StructType, field_name: str, new_type: DataType) -> StructType:
    new_fields = []
    
    # Iterate through each field in the schema
    for field in schema:
        # If the field name matches, replace its data type
        if field.name == field_name:
            new_fields.append(StructField(field.name, new_type, field.nullable))
        # If the field is a nested StructType, recursively replace the field type
        elif isinstance(field.dataType, StructType):
            new_struct = replace_field_type(field.dataType, field_name, new_type)
            new_fields.append(StructField(field.name, new_struct, field.nullable))
        # Otherwise, keep the field as is
        else:
            new_fields.append(field)
    
    # Return the new schema with the updated field type
    return StructType(new_fields)

# COMMAND ----------

# DBTITLE 1,Creating and Modifying Schema in PySpark DataFrame
from pyspark.sql.types import StructType, StringType

# Extract the schema of the JSON data from the DataFrame
old_schema = df.selectExpr("schema_of_json_agg(json_data) as schema").first()[0]

# Convert the extracted schema to a StructType
top_struct = StructType.fromDDL(old_schema)

# Replace the data type of the "content" field with StringType
new_schema = replace_field_type(top_struct, "content", StringType())

# Display the new schema
display(new_schema)

# COMMAND ----------

# DBTITLE 1,Parsing and Displaying JSON Data in a DataFrame
# Parse the JSON data in the "json_data" column using the new schema
parsed_df = df.withColumn("json_data", from_json(col("json_data"), new_schema))

# Display the resulting DataFrame
display(parsed_df)
