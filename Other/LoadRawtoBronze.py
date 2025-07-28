# Databricks notebook source
# MAGIC %run "/Workspace/Users/dbxdeschrtrial2@gmail.com/databricks/Other/common"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Utils

# COMMAND ----------

dbutils.widgets.text(name="env",defaultValue="",label=" Enter the environment in lower case")
global env
env = dbutils.widgets.get("env")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read traffic data

# COMMAND ----------

def read_Traffic_Data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Traffic Data :  ", end='')
    schematraffic = StructType([
    StructField("Record_ID",IntegerType()),
    StructField("Count_point_id",IntegerType()),
    StructField("Direction_of_travel",StringType()),
    StructField("Year",IntegerType()),
    StructField("Count_date",StringType()),
    StructField("hour",IntegerType()),
    StructField("Region_id",IntegerType()),
    StructField("Region_name",StringType()),
    StructField("Local_authority_name",StringType()),
    StructField("Road_name",StringType()),
    StructField("Road_Category_ID",IntegerType()),
    StructField("Start_junction_road_name",StringType()),
    StructField("End_junction_road_name",StringType()),
    StructField("Latitude",DoubleType()),
    StructField("Longitude",DoubleType()),
    StructField("Link_length_km",DoubleType()),
    StructField("Pedal_cycles",IntegerType()),
    StructField("Two_wheeled_motor_vehicles",IntegerType()),
    StructField("Cars_and_taxis",IntegerType()),
    StructField("Buses_and_coaches",IntegerType()),
    StructField("LGV_Type",IntegerType()),
    StructField("HGV_Type",IntegerType()),
    StructField("EV_Car",IntegerType()),
    StructField("EV_Bike",IntegerType())
    ])



    rawTraffic_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("checkpointLocation", f'{checkpoint_path}')
        .option('header','true')
        .schema(schematraffic)
        .load(f'{landingzone}/raw_traffic/')
        )

    ret_rawTraffic_stream = create_columnTime('Extract_Time',rawTraffic_stream)
     
    
    print('Reading Succcess !!')
    print('*******************')

    return ret_rawTraffic_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## write_Traffic_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Traffic_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_traffic table', end='' )
    write_Stream = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",f'{checkpoint_path}/raw_traffic/')
                    .outputMode('append')
                    .queryName('rawTrafficWriteStream')
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_traffic`"))
    
    write_Stream.awaitTermination()
    print('Write Success Traffic Data')
    print("****************************")    

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read road data

# COMMAND ----------

def read_roads_data():
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    from pyspark.sql.functions import current_timestamp
    print("Reading the Raw Road Data :  ", end='')
    schemaroad = StructType([
        StructField("Road_ID", IntegerType()),
        StructField("Road_Category_Id", IntegerType()),
        StructField("Road_Category", StringType()),
        StructField("Region_ID", IntegerType()),
        StructField("Region_Name", StringType()),
        StructField("Total_Link_Length_Km", DoubleType()),
        StructField("Total_Link_Length_Miles", DoubleType()),
        StructField("All_Motor_Vehicles", DoubleType())
    ])


    # structureType
    rawroads_stream = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","csv")
        .option("checkpointLocation", f'{checkpoint_path}/raw_roads/')
        .option('header','true')
        .schema(schemaroad)
        .load(f'{landingzone}/raw_roads/')
    )
    ret_rawroads_stream = create_columnTime('Extract_Time',rawroads_stream)
   
    
    print('Reading Succcess Road Data!')
    print('*******************')

    return ret_rawroads_stream

# COMMAND ----------

# MAGIC %md
# MAGIC ## write_Road_Data(StreamingDF,environment) Function

# COMMAND ----------

def write_Road_Data(StreamingDF,environment):
    print(f'Writing data to {environment}_catalog raw_road table', end='' )
    writeRoads_Stream = (StreamingDF.writeStream
                    .format('delta')
                    .option("checkpointLocation",f'{checkpoint_path}/raw_roads/')
                    #.option("cloudFiles.partitionColumns", "Region_ID")
                    .outputMode('append')
                    .queryName('rawroadsWriteStream')
                    .option("mergeSchema", "true")
                    .trigger(availableNow=True)
                    .toTable(f"`{environment}_catalog`.`bronze`.`raw_roads`"))
    
    writeRoads_Stream.awaitTermination()
    print('Write Success Road Data')
    print("****************************")   

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------


Initglobalvarpath(env)
print(landingzone)
read_Traffic_DF = read_Traffic_Data()
clean_Traffic_df = clean_spark_cols(read_Traffic_DF,'Traffic')

write_Traffic_Data(clean_Traffic_df ,env)

read_road_DF = read_roads_data()
clean_road_Data_cols = clean_spark_cols(read_road_DF,'Road')
write_Road_Data(clean_road_Data_cols,env)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from dev_catalog.bronze.raw_roads
