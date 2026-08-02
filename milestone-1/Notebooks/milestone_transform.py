# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

zomato = spark.read.format("parquet")\
          .option("header", "true")\
          .option("inferSchema", "true")\
          .load("/Volumes/workspace/default/day-2-dataset/zomato_cleaned")

# COMMAND ----------

zomato.count()

# COMMAND ----------

zomato.printSchema()
zomato.show(5)

# COMMAND ----------

# Creating ids 
zomato = zomato.withColumn("restro_id", monotonically_increasing_id())
zomato.show(5)

# COMMAND ----------

location_df = zomato.select("location").distinct()
location_df = location_df.withColumn("location_id", monotonically_increasing_id())
location_df.show()


# COMMAND ----------

cuisine_df = zomato.select("cuisines").distinct()
cuisine_df = cuisine_df.withColumn("cuisine_id", monotonically_increasing_id())
cuisine_df.show()


# COMMAND ----------

rating_df = zomato.select("rating", "votes").distinct()
rating_df = rating_df.withColumn("rating_id", monotonically_increasing_id())
rating_df.show()

# COMMAND ----------

#Joining the tables 
zomato_joined = zomato.join(location_df, on="location", how="inner")
zomato_joined = zomato_joined.join(cuisine_df, on="cuisines", how="inner")
zomato_joined = zomato_joined.join(rating_df, on=["rating", "votes"], how="inner")
zomato_joined.printSchema()

# COMMAND ----------

restaurant_df = zomato_joined.select("restro_id", "name", "location_id", "cuisine_id", "rating_id", "approx_cost_two", "rest_type")
restaurant_df.show()


# COMMAND ----------

restaurant_df.createOrReplaceTempView("restaurant")
location_df.createOrReplaceTempView("location")
cuisine_df.createOrReplaceTempView("cuisine")
rating_df.createOrReplaceTempView("rating")
spark.sql("SHOW TABLES").show()

# COMMAND ----------

# MAGIC %md
# MAGIC # PART C - sql

# COMMAND ----------

#Retrieve restaurant names along with their location names
spark.sql("""
           SELECT r.name, l.location  FROM restaurant r
           JOIN location l  ON r.location_id = l.location_id
          """).show(5)

# COMMAND ----------

#Find the top 10 restaurants with the highest ratings
spark.sql("""
          select r.name, ra.rating from restaurant r
          join rating ra on r.rating_id = ra.rating_id
          order by ra.rating desc
          """).show(10)

# COMMAND ----------

#Count the number of restaurants in each location
spark.sql("""
          select l.location, count(r.name) as no_of_restaurants from restaurant r 
          join location l on r.location_id = l.location_id
          group by l.location
          """).show()

# COMMAND ----------

#Identify cuisines with the highest average rating
spark.sql("""
          select c.cuisines, avg(ra.rating) as avg_rating from restaurant r 
          join cuisine c on r.cuisine_id = c.cuisine_id
          join rating ra on r.rating_id = ra.rating_id
          group by c.cuisines
          order by avg_rating desc
          """).show(5)

# COMMAND ----------

#List restaurants with cost above the average cost
spark.sql("""select avg(approx_cost_two) as avg_cost from restaurant """).show()
spark.sql("""
           select name, approx_cost_two as cost from restaurant 
           where approx_cost_two > (select avg(approx_cost_two) from restaurant)
          """).show()

# COMMAND ----------

#Find the total number of votes received per location
spark.sql("""
            select sum(ra.votes) as total_votes, l.location from restaurant  r
            join location l on r.location_id = l.location_id
            join rating ra on r.rating_id = ra.rating_id
            group by l.location
          """).show(10)

# COMMAND ----------

#Identify the most common cuisine in each location
spark.sql("""
          select c.cuisines , l.location from restaurant r 
          join cuisine c on r.cuisine_id = c.cuisine_id
          join location l on r.location_id = l.location_id
          group by c.cuisines, l.location
          order by count(c.cuisines) desc
          """).show(5)

# COMMAND ----------

#Retrieve restaurants that have ratings above 4 and votes greater than 500
spark.sql("""
          select r.name, ra.rating, ra.votes from restaurant r 
          join rating ra on r.rating_id = ra.rating_id
          where ra.rating > 4 and ra.votes > 500
          """).show(10)

# COMMAND ----------

#Find the average cost for two people by cuisine
spark.sql("""
           select c.cuisines, avg(r.approx_cost_two) as avg_cost_two from restaurant r 
           join cuisine c on r.cuisine_id = c.cuisine_id
           group by c.cuisines
          """).show(5)

# COMMAND ----------

#List locations where the number of restaurants exceeds a given threshold
spark.sql("""
           select l.location, count(r.name) as no_of_restro from restaurant r
           join location l on r.location_id = l.location_id 
           group by l.location 
           having count(r.name) > 500
          """).show(5)

# threshold value is 500 

# COMMAND ----------


