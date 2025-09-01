#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_series_python
# 
# New notebook

# In[12]:


import pandas as pd
from deltalake import write_deltalake, DeltaTable

# read csv file using pandas to dataframe
df_series = pd.read_csv("/lakehouse/default/Files/fabric_series_python/imdb_top_1000.csv")

# abfss file path to lakehouse table folder
table_path = "abfss://df2f57c6-ff7b-40a3-aa79-6f3bfb6af47f@onelake.dfs.fabric.microsoft.com/8dfd2611-e827-4887-949f-817a81411a56/Tables"

# delta table name
table_name = "series"

# storage options
storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}

# write data to delta table
write_deltalake(f"{table_path}/{table_name}", df_series, mode='overwrite', schema_mode='overwrite', engine='rust', storage_options=storage_options)

# read delta table to pandas dataframe
df_series_delta = DeltaTable(f"{table_path}/{table_name}", storage_options=storage_options).to_pandas()

limited_data = df_series_delta.head(10)

# display data
display(limited_data)


# In[14]:


import pandas as pd
import deltalake
import duckdb

# abfss file path to lakehouse table folder
table_path = "abfss://df2f57c6-ff7b-40a3-aa79-6f3bfb6af47f@onelake.dfs.fabric.microsoft.com/8dfd2611-e827-4887-949f-817a81411a56/Tables"

# delta table name
table_name = "series"

# storage options
storage_options = {"bearer_token": notebookutils.credentials.getToken("storage"), "use_fabric_endpoint": "true"}

# read delta table to pandas dataframe
df_series_delta = deltalake.DeltaTable(f"{table_path}/{table_name}", storage_options=storage_options).to_pandas()

# query data using duckdb
df_movies_delta_query = duckdb.sql("select * from df_series_delta").df()

# diplay data
display(df_movies_delta_query)

# use duckdb delta scan to query delta table directly
display(duckdb.sql("select Series_Title, Runtime, Poster_Link, Released_Year from delta_scan('/lakehouse/default/Tables/series')").df())

