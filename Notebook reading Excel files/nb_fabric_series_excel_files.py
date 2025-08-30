#!/usr/bin/env python
# coding: utf-8

# ## nb_fabric_series_excel_files
# 
# New notebook

# In[ ]:


import pandas as pd

lh_file_path = "abfss://df2f57c6-ff7b-40a3-aa79-6f3bfb6af47f@onelake.dfs.fabric.microsoft.com/8dfd2611-e827-4887-949f-817a81411a56/Files/fabric_series_excel_files"

# Read an excel file to a pandas DataFrame
df_pd = pd.read_excel(f"{lh_file_path}/file_1.xlsx")

display(df_pd)

# Convert to spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Write the spark df to lakehouse table
df_spark.write.mode("overwrite").format("delta").saveAsTable("fabric_series_file_1")

df_table = spark.sql("SELECT * FROM fabric_series_file_1")

display(df_table)


# In[1]:


import pandas as pd

lh_file_path = "abfss://df2f57c6-ff7b-40a3-aa79-6f3bfb6af47f@onelake.dfs.fabric.microsoft.com/8dfd2611-e827-4887-949f-817a81411a56/Files/fabric_series_excel_files"

df_pd = pd.read_excel(f"{lh_file_path}/file_2.xlsx", 
                        sheet_name="data_2",
                        header=None,
                        skiprows=3,
                        skipfooter=2,
                        names=["date","col1","col2","col3"])

df_spark = spark.createDataFrame(df_pd)

df_spark.write.mode("overwrite").format("delta").saveAsTable("fabric_series_file_2")

df_table = spark.sql("SELECT * FROM fabric_series_file_2")

display(df_table)

