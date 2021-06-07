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
# MAGIC   COMMENT 'This database connects to external MySQL tables';

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
# MAGIC mysql databases
# MAGIC """
# MAGIC 
# MAGIC def register_mysql_string(name : str,
# MAGIC                           comment : str,
# MAGIC                           host_name : str,
# MAGIC                           host_port : str,
# MAGIC                           host_database : str,
# MAGIC                           dbtable : str,
# MAGIC                           user : str,
# MAGIC                           password : str) -> str:
# MAGIC   return f"""CREATE TABLE IF NOT EXISTS loans.{name}
# MAGIC              USING org.apache.spark.sql.jdbc
# MAGIC              OPTIONS (
# MAGIC                driver 'org.mariadb.jdbc.Driver',
# MAGIC                url 'jdbc:mysql://{host_name}:{host_port}/{host_database}',
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
# MAGIC                driver 'org.mariadb.jdbc.Driver',
# MAGIC                url 'jdbc:mysql://ip_address:3306/loans',
# MAGIC                dbtable 'people',
# MAGIC                user 'user',
# MAGIC                password 'password'
# MAGIC                )
# MAGIC              COMMENT 'People Index'
# MAGIC           """
# MAGIC 
# MAGIC # Check the results of the template
# MAGIC assert register_mysql_string(name = "people",
# MAGIC                              comment = "People Index", 
# MAGIC                              host_name = "ip_address", 
# MAGIC                              host_port = "3306", 
# MAGIC                              host_database = "loans", 
# MAGIC                              dbtable = "people", 
# MAGIC                              user = "user", 
# MAGIC                              password = "password") == expected_sql_template

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## MySQL Setup

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
# MAGIC tables = ["people", "loan", "loan_application", "loan_application_params", "people_balance"]
# MAGIC comments = ['', '' , '', '', '']
# MAGIC ip_addr = ''
# MAGIC database = ''
# MAGIC port = 3306
# MAGIC user = ''
# MAGIC password = ''

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Test MySQL Connections

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Unit Test

# COMMAND ----------

# DBTITLE 1,Test MySQL Connection
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Check the mysql connections
# MAGIC """
# MAGIC 
# MAGIC for table,comment in zip(tables, comments):
# MAGIC   
# MAGIC   # jdbc url
# MAGIC   jdbc_url = f"""jdbc:mysql://{ip_addr}:{port}/{database}"""
# MAGIC   
# MAGIC   try:
# MAGIC     spark.read.format("jdbc")\
# MAGIC          .options(url = jdbc_url,
# MAGIC                   driver = "org.mariadb.jdbc.Driver",
# MAGIC                   query = f"""SELECT * FROM {table} LIMIT 10""",
# MAGIC                   user = user,
# MAGIC                   password = password)\
# MAGIC          .load()
# MAGIC   
# MAGIC   except Exception as e:
# MAGIC     print(f"Failed to read MySQL {table}, error : {str(e)}")
# MAGIC     break

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Register Tables

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Add MySQL Tables

# COMMAND ----------

# DBTITLE 1,Create Tables
# MAGIC %python
# MAGIC 
# MAGIC """
# MAGIC Register multiple mySQL tables
# MAGIC """
# MAGIC 
# MAGIC for table,comment in zip(tables, comments):
# MAGIC   
# MAGIC   # sql query
# MAGIC   sql_query = register_mysql_string(name = table,
# MAGIC                                     comment = comment,
# MAGIC                                     host_name = ip_addr ,
# MAGIC                                     host_port = port,
# MAGIC                                     host_database = database,
# MAGIC                                     dbtable = table,
# MAGIC                                     user = user,
# MAGIC                                     password = password)
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
