#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_series_parallel_api_calls
# 
# New notebook

# In[12]:

import requests
import os
import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

lakehouse_base_path = "/lakehouse/default/Files/fabric_series_pokemon_api"

base_url = "https://pokeapi.co/api/v2/"
endpoints = ["pokemon", "berry", "item-pocket", "berry"]
current_timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')

# Removes duplicates from the 'endpoints' list
endpoint_list = list(set(endpoints))

def process_endpoint(endpoint):
    root_folder = f"{lakehouse_base_path}/{endpoint}/{current_timestamp}"
    url = base_url + endpoint
    next_url = url
    page_counter = 1

    # Ensure the folder exists by creating the directory (automatically handles missing directories)
    os.makedirs(root_folder, exist_ok=True)

    while next_url:
        print(f"Endpoint {endpoint} API call index: {page_counter}")

        try:
            response = requests.get(next_url)
            data = response.json()

            with open(f"{root_folder}/{endpoint}_{page_counter}.json", "w") as f:
                f.write(json.dumps(data["results"]))

            next_url = data["next"]

            page_counter += 1
        except Exception as e:
            raise ValueError(f"An error occurred while processing {endpoint} at page {page_counter}: {e}")

    print(f"All {endpoint} pages processed!")

#for endpoint in endpoint_list:
#    process_endpoint(endpoint)

# Run endpoints in parallel and handle errors
failed = []
with ThreadPoolExecutor(max_workers=len(endpoint_list)) as executor:
    futures = {executor.submit(process_endpoint, endpoint): endpoint for endpoint in endpoint_list}

for future in as_completed(futures):
    endpoint = futures[future]
    try:
        result = future.result()
    except Exception as e:
        fail = f"{endpoint}: {e}"
        failed.append(fail)

if failed:
    print("The following errors occurred during execution:")
    for fail in failed:
        print(fail)
    raise ValueError(f"{len(failed)} error(s) occurred while processing")
else:
    print("The process finished successfully without errors.")
