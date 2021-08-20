# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Register database connections into the HIVE metastore for Databricks parallel data access

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Database Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Database

# COMMAND ----------

# DBTITLE 1,Register a loan database
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Create the loan database
# MAGIC --
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS LOANS
# MAGIC   COMMENT 'This database connects to external postgres tables';

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Create Template

# COMMAND ----------

# DBTITLE 1,Templating Function
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Create the templated sql string to register
# MAGIC postgres databases
# MAGIC """
# MAGIC 
# MAGIC def register_postgresql_string(name : str,
# MAGIC                                comment : str,
# MAGIC                                host_name : str,
# MAGIC                                host_port : str,
# MAGIC                                host_database : str,
# MAGIC                                dbtable : str,
# MAGIC                                user : str,
# MAGIC                                password : str) -> str:
# MAGIC   return f"""CREATE TABLE IF NOT EXISTS loans.{name}
# MAGIC              USING org.apache.spark.sql.jdbc
# MAGIC              OPTIONS (
# MAGIC                driver 'org.postgresql.Driver',
# MAGIC                url 'jdbc:postgresql://{host_name}:{host_port}/{host_database}',
# MAGIC                dbtable '{dbtable}',
# MAGIC                user '{user}',
# MAGIC                password '{password}'
# MAGIC                )
# MAGIC              COMMENT '{comment}'
# MAGIC           """

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Unit Test Template

# COMMAND ----------

# DBTITLE 1,Test Template
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Unit test the test template
# MAGIC """
# MAGIC 
# MAGIC # Test template results
# MAGIC expected_sql_template = """CREATE TABLE IF NOT EXISTS loans.people
# MAGIC              USING org.apache.spark.sql.jdbc
# MAGIC              OPTIONS (
# MAGIC                driver 'org.postgresql.Driver',
# MAGIC                url 'jdbc:postgresql://ip_address:5432/loans',
# MAGIC                dbtable 'people',
# MAGIC                user 'user',
# MAGIC                password 'password'
# MAGIC                )
# MAGIC              COMMENT 'People Index'
# MAGIC           """
# MAGIC 
# MAGIC # Check the results of the template
# MAGIC assert register_postgresql_string(name = "people",
# MAGIC                                   comment = "People Index", 
# MAGIC                                   host_name = "ip_address", 
# MAGIC                                   host_port = "5432", 
# MAGIC                                   host_database = "loans", 
# MAGIC                                   dbtable = "people", 
# MAGIC                                   user = "user", 
# MAGIC                                   password = "password") == expected_sql_template

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## RDS Setup

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Setup Parameters

# COMMAND ----------

# DBTITLE 1,Database Parameters
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Define the needed tables to register
# MAGIC """
# MAGIC 
# MAGIC tables = ["iris", "loan_data"]
# MAGIC comments = ['', '' , '', '', '']
# MAGIC ip_addr = dbutils.secrets.get( "oetrta", "postgres-hostname" )
# MAGIC port     = dbutils.secrets.get( "oetrta", "postgres-port"     )
# MAGIC database = dbutils.secrets.get( "oetrta", "postgres-database" )
# MAGIC user = dbutils.secrets.get( "oetrta", "postgres-username" )
# MAGIC password = dbutils.secrets.get( "oetrta", "postgres-password" )

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test Postgres Connections

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Unit Test

# COMMAND ----------

# DBTITLE 1,Test Postgres Connection
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Check the postgres connections
# MAGIC """
# MAGIC 
# MAGIC for table,comment in zip(tables, comments):
# MAGIC   
# MAGIC   # jdbc url
# MAGIC   jdbc_url = f"""jdbc:postgresql://{ip_addr}:{port}/{database}"""
# MAGIC   
# MAGIC   try:
# MAGIC     spark.read.format("jdbc")\
# MAGIC          .options(url = jdbc_url,
# MAGIC                   driver = "org.postgresql.Driver",
# MAGIC                   query = f"""SELECT * FROM {table} LIMIT 10""",
# MAGIC                   user = user,
# MAGIC                   password = password)\
# MAGIC          .load()
# MAGIC   
# MAGIC   except Exception as e:
# MAGIC     print(f"Failed to read RDS {table}, error : {str(e)}")
# MAGIC     break

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Register Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add Postgres Tables

# COMMAND ----------

# DBTITLE 1,Create Tables
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Register multiple Postgres tables
# MAGIC """
# MAGIC 
# MAGIC for table,comment in zip(tables, comments):
# MAGIC   
# MAGIC   # sql query
# MAGIC   sql_query = register_postgresql_string(name = table,
# MAGIC                                          comment = comment,
# MAGIC                                          host_name = ip_addr ,
# MAGIC                                          host_port = port,
# MAGIC                                          host_database = database,
# MAGIC                                          dbtable = table,
# MAGIC                                          user = user,
# MAGIC                                          password = password)
# MAGIC   try:
# MAGIC     spark.sql(sql_query)
# MAGIC #     print(sql_query)
# MAGIC   except Exception as e:
# MAGIC     print(f"Failed to create {table}, error : {str(e)}")
# MAGIC     break

# COMMAND ----------

# DBTITLE 1,Show all Tables in Database
# MAGIC %sql
# MAGIC 
# MAGIC --
# MAGIC -- Get the tables for the loan
# MAGIC --
# MAGIC 
# MAGIC SHOW TABLES FROM LOANS;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Test Tables

# COMMAND ----------

# DBTITLE 1,Check to make sure each external DB is queryable
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Validate reading from databases
# MAGIC """
# MAGIC 
# MAGIC for table in tables:
# MAGIC   try:
# MAGIC     spark.sql(f"""SELECT * FROM LOANS.{table} LIMIT 10""")
# MAGIC   except Exception as e:
# MAGIC     print("Failed to read from loans.{table}, error : {e}")
