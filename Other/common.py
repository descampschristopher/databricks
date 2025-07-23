# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Re-Using functions
# MAGIC - We can re-use functions here
# MAGIC   - Removing Duplicates
# MAGIC   - Removing NULLs
# MAGIC   - Extract Time field
# MAGIC   - Tranformed Time field

# COMMAND ----------

# MAGIC %md
# MAGIC # Variables

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- Initiate Variables

# COMMAND ----------

def Initglobalvarpath(environment):
    global checkpoint_path
    global landingzone 
    checkpoint_path =f"/Volumes/{environment}_catalog/bronze/checkpoint/"
    landingzone = f"/Volumes/{environment}_catalog/bronze/landingzone"
    return 1

# COMMAND ----------

# MAGIC %md
# MAGIC # Define function

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- Handling NULL 

# COMMAND ----------

def handle_NULLs(df,columns):
    print('Replacing NULL values on String Columns with "Unknown" ' , end='')
    df_string = df.fillna('Unknown',subset= columns)
    print('Successs!! ')

    print('Replacing NULL values on Numeric Columns with "0" ' , end='')
    df_clean = df_string.fillna(0,subset = columns)
    print('*******************************************************')
    print('Success!! ')

    return df_clean

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- Creating Time column
# MAGIC

# COMMAND ----------

def create_columnTime(name,df):
    from pyspark.sql.functions import current_timestamp
    print('Creating {name} Time column : ',end='')
    df_timestamp = df.withColumn('Transformed_Time',
                      current_timestamp()
                      )
    print('*******************************************************')
    print('Success!!')
    return df_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ## -- Removing duplicates

# COMMAND ----------

def remove_Dups(df):
    print('Removing Duplicate values: ',end='')
    df_dup = df.dropDuplicates()
    print('Success!')
    return df_dup
