# snowflakePOC
This is a quick POC doing an extract of a customer contact table from oracle then loading it to snowflake using a parquet file.

# Envionment Variables that must be set:

*These are referenced in dbconfig.js.*

NODE_ORACLEDB_USER
NODE_ORACLEDB_PASSWORD
NODE_ORACLEDB_CONNECTIONSTRING
NODE_ORACLEDB_EXTERNALAUTH (Optional)