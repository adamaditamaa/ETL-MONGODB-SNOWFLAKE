from pymongo import MongoClient

# MongoDB connection parameters

mongo_uri = "mongodb+srv://doadmin:MJiW5Af42z860u13@db-mongodb-sgp1-37041-f1977558.mongo.ondigitalocean.com/"
db_name = "adam_adam"
collection_name = "invoice"

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Extract data
mongo_data = list(collection.find())

import pandas as pd

df = pd.DataFrame(mongo_data)

import snowflake.connector


# Snowflake connection parameters
snowflake_user = "adamaditama"
snowflake_password = "@Balola021"
snowflake_account = "ju91287.ap-southeast-3"
snowflake_database = "adam"
snowflake_schema = "PUBLIC"
snowflake_role = "ACCOUNTADMIN"
snowflake_warehouse = "all_data"

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=snowflake_user,
    password=snowflake_password,
    account=snowflake_account,
    warehouse=snowflake_warehouse,
    database=snowflake_database,
    schema=snowflake_schema,
    role=snowflake_role
)


df.to_sql('invoice',con=conn,if_exists='append', index=False)




