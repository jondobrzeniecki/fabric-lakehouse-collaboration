# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4c9ba57b-f0a6-4e1e-a6ff-021eb70fc836",
# META       "default_lakehouse_name": "lh1",
# META       "default_lakehouse_workspace_id": "50256700-cd75-43bf-ae56-1f0a129e7f0b",
# META       "known_lakehouses": [
# META         {
# META           "id": "4c9ba57b-f0a6-4e1e-a6ff-021eb70fc836"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Create a dummy table dummy_table = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"]) and call it Table0
dummy_table = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])
dummy_table.createOrReplaceTempView("Table0")
display(dummy_table)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# write dummy_table to lh1 under the dbo schema as Table0
dummy_table.write.format("delta").mode("overwrite").saveAsTable("dbo.Table0")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM dbo.Table0

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
