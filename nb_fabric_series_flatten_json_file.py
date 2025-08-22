#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_series_flatten_json_file
# 
# New notebook

# In[56]:


df = spark.read.option("multiline", "true").json("Files/fabric_series/zoo.json")
# df now is a Spark DataFrame containing JSON data from "Files/fabric_series/zoo.json".
df.createOrReplaceTempView("df_zoo_view")


# In[57]:


display(spark.sql("""
SELECT explode(zoo) as zoo FROM df_zoo_view
"""))


# In[58]:


display(spark.sql("""
SELECT z.zoo.*
FROM (
    SELECT explode(zoo) as zoo FROM df_zoo_view
) as z
"""))


# In[59]:


df_zoo = spark.sql("""
SELECT z.zoo.zoo_id, z.zoo.state, z.zoo.country, z.zoo.city
FROM (
    SELECT explode(zoo) as zoo FROM df_zoo_view
) as z
""")
df_zoo.write.mode("overwrite").format("delta").saveAsTable("zoo")


# In[60]:


df_animals = spark.sql("""
SELECT 
 a.zoo_id
,a.animals.animal_id 
,a.animals.name, a.animals.species
,a.animals.characteristics.diet
,a.animals.characteristics.lifespan.average_years
,a.animals.characteristics.lifespan.in_wild
,a.animals.characteristics.lifespan.in_captivity
,a.animals.characteristics.weight_kg.male
,a.animals.characteristics.weight_kg.female

FROM (
    SELECT z.zoo.zoo_id, explode(z.zoo.animals) as animals
FROM (
    SELECT explode(zoo) as zoo FROM df_zoo_view
) as z
) as a
""")
df_animals.write.mode("overwrite").format("delta").saveAsTable("animals")


# In[61]:


df_habitat = spark.sql("""
SELECT 
 a.zoo_id
,a.animals.animal_id 
,explode(a.animals.characteristics.habitat) habitat

FROM (
    SELECT z.zoo.zoo_id, explode(z.zoo.animals) as animals
FROM (
    SELECT explode(zoo) as zoo FROM df_zoo_view
) as z
) as a
""")
df_habitat.write.mode("overwrite").format("delta").saveAsTable("habitat")

