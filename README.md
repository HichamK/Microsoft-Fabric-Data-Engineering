# Microsoft Fabric JSON Notebooks

Ce dépôt contient des scripts Python et notebooks pour traiter des données JSON et interagir avec des APIs dans **Microsoft Fabric Notebooks**.  
L’objectif est de faciliter l’ingestion, la transformation et l’analyse de données complexes provenant d’APIs ou de fichiers JSON.

---

## Fichiers actuels

1. **nb_fabric_series_fetch_data_from_api.py**  
   - Récupère des données JSON paginées depuis une API.  
   - Traite les données et les écrit dans un Lakehouse.  

2. **nb_fabric_series_flatten_json_file.py**  
   - Traite et aplatit des fichiers JSON complexes (arrays, objets imbriqués).  
   - Prépare les données pour l’analyse.
     
3. **Fetch API Data Faster: Parallel API Calls in Microsoft Fabric Notebooks**  
   - Exemple d’utilisation de requêtes parallèles pour accélérer la récupération de données depuis des APIs. 
---

## Fichiers à venir

- **Notebook reading Excel files**  
  - Moves data from an Excel file in Azure Lakehouse into a managed Delta table in Spark.  

---

## Prérequis

- **Microsoft Fabric Notebooks** (compte Microsoft nécessaire)  
- Accès à un **Lakehouse** si les scripts écrivent des données  
- Connaissances de base en **PySpark** pour exécuter et modifier les notebooks
