# Microsoft Fabric Notebooks for Data Engineering

This repository contains Python scripts and notebooks designed to process JSON data and interact with APIs within Microsoft Fabric Notebooks.
The goal is to simplify data ingestion, transformation, and analysis of complex datasets coming from APIs or JSON files.

---

## Current Files

1. **nb_fabric_series_fetch_data_from_api.py**  
   - Retrieves paginated JSON data from an API.  
   - Processes the data and writes it into a Lakehouse.  

2. **nb_fabric_series_flatten_json_file.py**  
   - Processes and flattens complex JSON files (arrays, nested objects).  
   - Prepares the data for analysis.
     
3. **Fetch API Data Faster: Parallel API Calls in Microsoft Fabric Notebooks**  
   - Example of using parallel requests to speed up data retrieval from APIs. 
---

## Upcoming Files

- **Notebook reading Excel files**  
  - Moves data from an Excel file in Azure Lakehouse into a managed Delta table in Spark.  

---

## Prerequisites

- **Microsoft Fabric Notebooks** (requires a Microsoft account)  
- Access to a **Lakehouse** if scripts are writing data  
- Basic knowledge of **PySpark** to run and adapt the notebooks
