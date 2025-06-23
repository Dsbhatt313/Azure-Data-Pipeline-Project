# 🚀 Azure Data Pipeline Project

## 📌 Project Overview

This project demonstrates a complete end-to-end data pipeline architecture using Microsoft Azure services. The pipeline is designed to fetch data from a GitHub API source, store it in a structured data lake environment (bronze, silver, and gold layers), perform data transformation and analysis using Azure Databricks, and finally query the processed data using Azure Synapse Analytics.

It showcases how various Azure components work together to build a scalable, modular, and production-ready data engineering solution.

---

## 🔧 Technologies Used

- **Azure Data Factory (ADF):** For orchestrating data ingestion from external sources (GitHub HTTP API).
- **Azure Data Lake Storage Gen2:** To store data in bronze (raw), silver (cleaned), and gold (aggregated) formats.
- **Azure Databricks:** For data transformation, cleaning, and analysis using PySpark.
- **Azure Synapse Analytics:** For querying and analyzing the final processed data.
- **GitHub:** Used as the external source of raw JSON data.
- **Python (Databricks Notebooks)** and **SQL (Synapse Analytics)**

---

## 📂 Folder Structure

azure-data-pipeline-project/
│
├── README.md # Project documentation
├── architecture.png # Diagram of the entire pipeline
│
├── data_factory/
│ └── DynamicGitToBronze.json # Exported JSON of the ADF pipeline
│
├── datalake/
│ └── structure.md # Description of bronze, silver, and gold layers
│
├── databricks_notebook/
│ └── Silver-Layer_Transformation.py # PySpark transformation notebook
│
├── synapse_sql_scripts/
│ ├── CreateSchema.sql
│ ├── CreateExternalTable.sql
│ └── CreateViewsGold.sql


---

## 🔄 Data Flow

[GitHub HTTP Endpoint]
↓
[Azure Data Factory]
↓
[Azure Data Lake - Bronze Layer]
↓
[Azure Databricks]
↓ ↓
[Silver Layer] ← Cleaned & transformed data
↓
[Gold Layer] ← Aggregated, analytics-ready data
↓
[Azure Synapse Analytics]


---

## ⚙️ Component Breakdown

### 1. **Azure Data Factory**
- Fetches JSON data from a GitHub HTTP endpoint using a linked service.
- Loads the raw data into the **bronze** container in Azure Data Lake.
- The pipeline JSON is exported as `DynamicGitToBronze.json` in the `data_factory/` folder.

### 2. **Azure Data Lake Storage Gen2**
- **Bronze Layer:** Stores raw ingested data.
- **Silver Layer:** Stores cleaned and transformed data.
- **Gold Layer:** Stores final aggregated and analytics-ready datasets.

### 3. **Azure Databricks**
- Reads data from the bronze layer.
- Applies data cleaning, formatting, and transformation using PySpark.
- Outputs results to the silver and gold layers.
- Notebook is saved as `Silver-Layer_Transformation.py` in the `databricks_notebook/` folder.

### 4. **Azure Synapse Analytics**
- Reads the gold layer for querying and analysis.
- SQL scripts for schema creation, external table creation, and view generation are stored in the `synapse_sql_scripts/` folder.

---

## 📷 Architecture Diagram

Include a visual representation of the pipeline architecture, showing data flow across services. Save it as `architecture.png`.

---

## 🧪 How to Run This Project

1. **Create a Resource Group** in Azure and add the following services:
   - Azure Data Factory
   - Azure Data Lake Storage Gen2
   - Azure Databricks Workspace
   - Azure Synapse Analytics Workspace

2. **Import the ADF Pipeline:**
   - Use the exported JSON file `DynamicGitToBronze.json` from the `data_factory/` folder.

3. **Set Up Data Lake Containers:**
   - Create three containers: `bronze/`, `silver/`, and `gold/`.

4. **Run Databricks Notebook:**
   - Upload and execute the PySpark notebook from `databricks_notebook/`.

5. **Execute SQL Scripts in Synapse:**
   - Use the scripts from the `synapse_sql_scripts/` folder to create schema, external tables, and views on the gold layer data.

---


## 👤 Author

**Dhruvansh Bhatt**  
*Data Science Intern | Passionate about Data Engineering & Finance*  
[LinkedIn](https://www.linkedin.com/in/dhruvansh-bhatt) • [GitHub](https://github.com/yourusername)

---

## 📄 License

This project is intended for educational and demonstration purposes only.
