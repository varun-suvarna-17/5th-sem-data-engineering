# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 

# COMMAND ----------

zomato = spark.read.format("csv")\
         .option("header", "true")\
        .option("multiLine", "true")\
         .option("quote", '"') \
        .option("escape", '"') \
        .option("inferSchema", "true")\
        .load("/Volumes/workspace/default/day-2-dataset/zomato.csv")

# COMMAND ----------

#checking the data of dataframe 
zomato.printSchema()
zomato.show(5)

# COMMAND ----------

#Exploring the data in dataframe 
print("Total no of rows: ", zomato.count())
print("Total no of columns: ", len(zomato.columns))
print("distinct rows: ", zomato.distinct().count())

# COMMAND ----------

#Standardising the columns 
zomato =  zomato.withColumnsRenamed({
    "approx_cost(for two people)": "approx_cost_two",
    "listed_in(city)" : "listed_in_city",
    "listed_in(type)" : "listed_in_type",
    "rate" : "rating"
})
zomato.columns

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Missing Value Handling

# COMMAND ----------

print("Missing values in each column:")
for c in zomato.columns:
    print(c, zomato.filter(col(c).isNull()).count())


# COMMAND ----------

zomato = zomato.na.drop(subset=["location", "cuisines", "listed_in_city", "listed_in_type", "menu_item", "approx_cost_two"])

print("after dropping null : ", zomato.count())

zomato = zomato.na.fill("Unknown", subset=['dish_liked', 'rest_type', 'phone' ])

print("Missing values in each column:")
for c in zomato.columns:
    print(c, zomato.filter(col(c).isNull()).count())



# COMMAND ----------

# MAGIC %md
# MAGIC # Colum by Colum cleaning

# COMMAND ----------

print("Unique values in each column:")
for c in zomato.columns:
    print(c, zomato.select(c).distinct().count())



# COMMAND ----------

#zomato.select("online_order", "book_table", "rating", "votes", "listed_in_city", "listed_in_type").distinct().show()
#UNIQUE VALUES 
zomato.select("online_order").distinct().show()

zomato.select("book_table").distinct().show()

zomato.select("rating").distinct().show()

zomato.select("votes").distinct().show()

zomato.select("listed_in_city").distinct().show()

zomato.select("listed_in_type").distinct().show()

zomato.select("dish_liked").distinct().show()

zomato.select("approx_cost_two").distinct().show()




# COMMAND ----------

#rating changes 
zomato = zomato.replace(["NEW","-", "Unknown"] ,None, subset=["rating"])
zomato = zomato.withColumn("rating", regexp_replace(col("rating"), "/5", ''))

#result
zomato.select("rating").distinct().show()
zomato.printSchema()

# COMMAND ----------

#datatype 
zomato = zomato.withColumn("rating", col("rating").cast("float"))
#zomato = zomato.withColumn("votes", regexp_replace(col("votes"), ",", ''))
zomato = zomato.withColumn("votes", col("votes").cast("int"))
#zomato = zomato.withColumn("approx_cost_two", regexp_replace(col("approx_cost_two"), ",", ''))
zomato = zomato.withColumn("approx_cost_two",col("votes").cast("float"))

#result
zomato.printSchema()


# COMMAND ----------

#final check 
zomato.printSchema()
print("Missing values in each column:")
for c in zomato.columns:
    print(c, zomato.filter(col(c).isNull()).count())


# COMMAND ----------

zomato.count()

# COMMAND ----------

zomato.write \
    .mode("overwrite") \
    .parquet("/Volumes/workspace/default/day-2-dataset/zomato_cleaned")