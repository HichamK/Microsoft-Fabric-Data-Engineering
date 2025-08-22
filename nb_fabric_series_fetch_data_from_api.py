#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_series_fetch_data_from_api
# 
# New notebook

# In[11]:


import requests
import os
import json

lakehouse_path = "/lakehouse/default/Files/fabric_series_pokemon_api"

# Ensure the folder exists by creating the directory (automatically handles missing directories)
os.makedirs(lakehouse_path, exist_ok=True)

response = requests.get("https://pokeapi.co/api/v2/pokemon")
# print(type(response.json()))
data = response.json()

with open(f"{lakehouse_path}/pokemon.json", "w") as f:
        f.write(json.dumps(data["results"]))


# In[ ]:


df = spark.read.option("multiline", "true").json("Files/fabric_series_pokemon_api/pokemon.json")
# df now is a Spark DataFrame containing JSON data from "Files/fabric_series_pokemon_api/pokemon.json".
display(df)


# In[ ]:


import requests
import os
import json
from datetime import datetime

lakehouse_base_path = "/lakehouse/default/Files/fabric_series_pokemon_api"

base_url = "https://pokeapi.co/api/v2/"
endpoint = "pokemon"
root_folder = f"{lakehouse_base_path}/{endpoint}"
url = base_url + endpoint
next_url = url
page_counter = 1

# Ensure the folder exists by creating the directory (automatically handles missing directories)
os.makedirs(root_folder, exist_ok=True)

while next_url:
    print(f"Endpoint {endpoint} API call index: {page_counter}")

    response = requests.get(next_url)
    data = response.json()

    with open(f"{root_folder}/{endpoint}_{page_counter}.json", "w") as f:
        f.write(json.dumps(data["results"]))

    next_url = data["next"]

    page_counter += 1

print(f"All {endpoint} pages processed!")


# In[ ]:


import requests
import os
import json
from datetime import datetime

lakehouse_base_path = "/lakehouse/default/Files/fabric_series_pokemon_api"

base_url = "https://pokeapi.co/api/v2/"
endpoint_list = ["pokemon", "berry"]

for endpoint in endpoint_list:
    root_folder = f"{lakehouse_base_path}/{endpoint}"
    url = base_url + endpoint
    next_url = url
    page_counter = 1

    # Ensure the folder exists by creating the directory (automatically handles missing directories)
    os.makedirs(root_folder, exist_ok=True)

    while next_url:
        print(f"Endpoint {endpoint} API call index: {page_counter}")

        response = requests.get(next_url)
        data = response.json()

        with open(f"{root_folder}/{endpoint}_{page_counter}.json", "w") as f:
            f.write(json.dumps(data["results"]))

        next_url = data["next"]

        page_counter += 1

    print(f"All {endpoint} pages processed!")


# In[ ]:


df = spark.read.option("multiline", "true").json("Files/fabric_series_pokemon_api/berry/berry_*.json")
# df now is a Spark DataFrame containing JSON data from "Files/fabric_series_pokemon_api/berry/berry_1.json".
display(df)

