# Microsoft Fabric Notebooks for Data Engineering

This repository contains Python scripts and notebooks designed to process JSON data and interact with APIs within Microsoft Fabric Notebooks.
The goal is to simplify data ingestion, transformation, and analysis of complex datasets coming from APIs or JSON files.

---

## Current Files

1. **REST API Pagination in Microsoft Fabric Notebooks/nb_fabric_series_fetch_data_from_api.py**  
   - Retrieves paginated JSON data from an API.  
   - Processes the data and writes it into a Lakehouse.  

2. **Processing and Flattening JSON Files/nb_fabric_series_flatten_json_file.py**  
   - Processes and flattens complex JSON files (arrays, nested objects).  
   - Prepares the data for analysis.
     
3. **Notebook Parallel API Calls/nb_fabric_series_parallel_api_calls.py**  
   - Using parallel requests to speed up data retrieval from APIs.

4. **Notebook reading Excel files/nb_fabric_series_excel_files.py**  
   - Notebook reading Excel files & write the spark df to lakehouse table
  
5. **Vanilla Python Notebooks/nb_fabric_series_python.py**  
   - How to use Python Notebooks for lightweight data operations in Fabric
   - How to interact with Lakehouse files and Delta tables without Spark
---

## Upcoming Files

- **Build Dynamic & Fast Notebooks**
- **How to handle column changes in files in Microsoft Fabric notebooks**
- **Python Terminal Colors and Text Styling**

---

## Prerequisites

- **Microsoft Fabric Notebooks** (requires a Microsoft account)  
- Access to a **Lakehouse** if scripts are writing data  
- Basic knowledge of **PySpark** to run and adapt the notebooks
