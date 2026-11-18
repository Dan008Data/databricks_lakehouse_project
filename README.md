
# End-to-End Azure Databricks Lakehouse Project

## 1. Project Overview

This project demonstrates the design and implementation of an end-to-end **Lakehouse Data Engineering Solution** for a product-based business using Azure Databricks, Azure Data Lake Storage Gen2, Delta Lake, and the Medallion Architecture.

The solution processes business data received in Parquet format, covering Customers, Products, Orders, and Regions. It implements incremental data ingestion, data cleansing and enrichment, dimensional modelling, Change Data Capture (CDC), and data pipeline orchestration.

The objective is to transform raw operational data into reliable, analytics-ready datasets that business analysts can use for reporting and decision-making.

The architecture follows the Medallion approach, separating data processing into Bronze, Silver, and Gold layers. Unity Catalog provides centralized data governance, while Databricks SQL provides the analytical access layer.

---

## 2. Client Requirements

The business requires a scalable and maintainable data platform capable of processing its operational data and preparing it for business intelligence and reporting.

The following requirements were identified for the solution:

### Data Ingestion and Storage
- Ingest Parquet datasets containing Customers, Products, Orders, and Regions from GitHub and Azure Data Lake Storage Gen2.
- Implement incremental data ingestion to process newly arriving files without continuously reprocessing historical data.
- Automate ingestion using Apache Spark Structured Streaming and Auto Loader.
- Store processed data in Delta Lake format.

### Data Transformation and Quality
- Implement the Medallion Architecture to separate raw, cleansed, and business-ready datasets.
- Clean and standardize incoming data.
- Enrich datasets with additional business attributes and calculated fields.
- Implement reusable PySpark functions and classes to simplify transformations.
- Apply data quality expectations to ensure required fields are valid.

### Data Modelling and Historical Tracking
- Build a Star Schema consisting of dimension and fact tables.
- Implement Slowly Changing Dimension Type 1 (SCD Type 1) for customer data to maintain the latest customer information.
- Implement Slowly Changing Dimension Type 2 (SCD Type 2) for product data to preserve historical changes.
- Implement incremental upsert logic to process new and updated records.
- Maintain surrogate keys and record creation/update timestamps.

### Pipeline Orchestration
- Automate the complete data processing workflow.
- Configure dependencies between ingestion, transformation, and dimensional modelling tasks.
- Allow independent Silver Layer transformations to execute in parallel.
- Ensure downstream Gold Layer processing starts only after its required upstream tasks have completed.

### Governance and Analytics
- Implement Unity Catalog for centralized data governance and access management.
- Provide a Databricks SQL Warehouse for analytical queries.
- Make Gold Layer datasets available for business intelligence and reporting through Power BI.

---

## 3. Final Deliverables

The completed project provides the following deliverables:

1. **Azure Cloud Infrastructure:** Azure Databricks workspace, Azure Data Lake Storage Gen2, and an Access Connector configured for secure storage access.
2. **Unity Catalog Governance:** Metastore, catalog, schemas, storage credentials, external locations, and workspace-level data access configuration.
3. **Automated Bronze Ingestion:** A reusable, parameter-driven ingestion framework using Auto Loader and Spark Structured Streaming to incrementally ingest multiple Parquet datasets.
4. **Silver Transformation Layer:** Cleansed and enriched Delta tables for Customers, Products, Orders, and Regions, supported by reusable PySpark functions and window-function logic.
5. **Gold Dimensional Model:** A Star Schema containing customer and product dimensions and an order fact table.
6. **SCD Type 1 Implementation:** PySpark-based customer dimension processing with initial loading, incremental processing, surrogate key management, and upsert logic.
7. **SCD Type 2 Implementation:** Product dimension processing using Lakeflow Spark Declarative Pipelines and the AUTO CDC API to maintain historical records.
8. **End-to-End Orchestration:** A Lakeflow Job coordinating Bronze ingestion, parallel Silver transformations, Gold dimension processing, and fact table creation.
9. **Analytics Layer:** A Databricks SQL Warehouse providing access to the Gold Layer for analytical queries.
10. **Power BI Reporting Readiness:** Gold Layer datasets structured for business intelligence and visualization.

---

## 4. Solution Architecture

The solution follows the Medallion Architecture, with each layer serving a specific purpose in the data engineering workflow.

![End-to-End Azure Databricks Lakehouse Architecture](images/end-to-end-pipeline.png)

### Architecture Flow

**Data Sources → Bronze → Silver → Gold → Databricks SQL Warehouse → Power BI**

| Layer | Purpose | Technologies |
|---|---|---|
| Sources | Store and provide raw business datasets | GitHub, Azure Data Lake Storage Gen2, Parquet |
| Bronze | Incremental ingestion and raw data preservation | Auto Loader, Structured Streaming, Lakeflow Jobs |
| Silver | Data cleansing, transformation, and enrichment | PySpark, Spark SQL, Delta Lake |
| Gold | Dimensional modelling and historical tracking | PySpark, Lakeflow Spark Declarative Pipelines, AUTO CDC |
| Warehouse | Analytical access to curated datasets | Databricks SQL Warehouse |
| Reporting | Business intelligence and visualization | Power BI |
| Governance | Centralized data access and permissions | Unity Catalog |

---

## 5. Technology Stack

| Technology | Purpose |
|---|---|
| Microsoft Azure | Cloud infrastructure |
| Azure Databricks | Data engineering and Lakehouse platform |
| Azure Data Lake Storage Gen2 | Data lake storage |
| Azure Databricks Access Connector | Secure access to Azure storage |
| Unity Catalog | Data governance and access management |
| Apache Spark | Distributed data processing |
| PySpark | Data transformations and business logic |
| Spark Structured Streaming | Incremental data processing |
| Auto Loader | Automated file ingestion |
| Delta Lake | Reliable storage and ACID transactions |
| Lakeflow Jobs | Workflow orchestration |
| Lakeflow Spark Declarative Pipelines | Declarative pipeline development and CDC |
| Databricks SQL Warehouse | Analytical querying |
| Power BI | Business intelligence and reporting |
| GitHub | Source control and project versioning |

---

## 6. Azure Infrastructure and Unity Catalog Configuration

The project infrastructure was provisioned in Microsoft Azure.

### Azure Resources

The following resources were configured:

- Azure Resource Group to organize project resources.
- Azure Databricks workspace for data engineering development.
- Azure Data Lake Storage Gen2 for source data and Delta Lake storage.
- Azure Databricks Access Connector to provide managed identity-based access to storage.

### Unity Catalog Configuration

Unity Catalog was configured to provide centralized governance across the Lakehouse.

The configuration included:

1. Creating a Unity Catalog metastore.
2. Configuring the metastore's managed storage location.
3. Assigning the metastore to the Databricks workspace.
4. Configuring the Azure Access Connector and required storage permissions.
5. Creating storage credentials.
6. Creating external locations for the source, Bronze, Silver, and Gold storage paths.
7. Creating the project catalog and corresponding schemas.
8. Configuring workspace-level permissions.

This provides a governed environment for managing data access and storage across the different Lakehouse layers.

---

## 7. Source Datasets

The project uses four business datasets in Parquet format.

| Dataset | Description |
|---|---|
| Customers | Customer information, including names and email addresses |
| Products | Product information and pricing |
| Orders | Customer transactions and order information |
| Regions | Regional business information |

The datasets are organized in separate source directories in Azure Data Lake Storage Gen2.

---

## 8. Bronze Layer – Incremental Data Ingestion

The Bronze Layer is responsible for ingesting raw data into the Lakehouse while preserving the source data for downstream processing.

### Dynamic and Parameter-Driven Ingestion

Instead of creating separate ingestion notebooks for Customers, Products, and Orders, I developed a reusable Bronze ingestion framework.

The framework consists of:

- A parameter notebook containing an array of dataset configurations.
- A reusable Bronze ingestion notebook.
- A Lakeflow Job with a For Each task to iterate through the configured datasets.

The parameter notebook uses `dbutils.jobs.taskValues.set()` to pass the dataset configurations to the ingestion task.

The Bronze ingestion notebook receives the dataset name as a parameter through `dbutils.widgets` and dynamically reads the corresponding source directory.

### Auto Loader and Structured Streaming

Auto Loader was implemented to detect and process newly arriving Parquet files.

The ingestion process uses:

- Spark Structured Streaming.
- Auto Loader for incremental file discovery.
- Append output mode.
- `trigger(availableNow=True)` to process available files and terminate after the available data has been processed.

This approach avoids maintaining a continuously running stream while still supporting incremental ingestion.

### Bronze Ingestion Job Pipeline

The Bronze ingestion workflow uses a For Each task to execute the reusable ingestion notebook for each configured dataset.

![Bronze Layer Ingestion Job Pipeline – Overview](images/bronze-ingestion-job-1.png)

![Bronze Layer Ingestion Job Pipeline – Task Configuration](images/bronze-ingestion-job-2.png)

The resulting Bronze datasets serve as the raw data foundation for the Silver Layer.

---

## 9. Silver Layer – Data Cleansing and Enrichment

The Silver Layer transforms raw Bronze data into clean, structured, and enriched datasets.

Separate PySpark notebooks were created for Customers, Products, Orders, and Regions because each dataset has different transformation requirements.

### 9.1 Silver Customers

The Customers transformation includes:

- Reading customer data from the Bronze Layer.
- Removing the `_rescued_data` column generated by Auto Loader.
- Extracting email domains from the email column using the `split()` function.
- Creating a new `domain` column to identify customer email providers.
- Grouping customers by email domain and calculating the total number of customers per domain.
- Filtering selected email domains, including Gmail, Yahoo, and Hotmail.
- Combining first and last names into a new `full_name` column using `concat()`.
- Dropping the original first and last name columns.

The transformed dataset is saved as the Delta table:

`customer_silver`

### 9.2 Silver Products

The Products transformation includes:

- Reading product data from the Bronze Layer.
- Creating reusable discount logic using a User Defined Function (UDF).
- Applying discount calculations to product pricing.
- Using the PySpark `expr()` function to invoke the SQL UDF.

The transformed dataset is saved as:

`products_silver`

### 9.3 Silver Orders

The Orders transformation includes:

- Reading order data from the Bronze Layer.
- Removing the `_rescued_data` column.
- Converting `order_date` into a timestamp data type.
- Extracting the year from the order date.
- Applying window functions to rank orders by `total_amount` within each year.
- Implementing a reusable window-function class containing:
  - Dense Rank
  - Rank
  - Row Number

This reusable class avoids repeatedly writing window-function logic across notebooks.

The transformed dataset is saved as:

`orders_silver`

### 9.4 Silver Regions

The Regions transformation includes:

- Reading region data from the Bronze Layer.
- Removing the `_rescued_data` column.
- Preparing the dataset for downstream dimensional modelling.

The transformed dataset is saved as:

`region_silver`

### Silver Layer Output

| Dataset | Delta Table |
|---|---|
| Customers | `customer_silver` |
| Products | `products_silver` |
| Orders | `orders_silver` |
| Regions | `region_silver` |

---

## 10. Gold Layer – Dimensional Modelling

The Gold Layer contains business-ready datasets designed for analytical querying and reporting.

A Star Schema was implemented using dimension and fact tables.

The dimensional model includes:

- `DimCustomers` – Customer dimension using SCD Type 1.
- `DimProducts` – Product dimension using SCD Type 2.
- `FactOrders` – Order fact table containing measures and dimension surrogate keys.

### 10.1 Customer Dimension – SCD Type 1

The customer dimension implements Slowly Changing Dimension Type 1 using PySpark.

SCD Type 1 overwrites existing dimension attributes when changes occur. It does not retain historical versions of those attributes.

The implementation was designed to support both initial loading and incremental processing.

#### Initial Load and Incremental Load

An `init_load_flag` parameter controls the loading process:

- `1` – Initial load.
- `0` – Incremental load.

#### Surrogate Key Management

A surrogate key, `DimCustomerKey`, is assigned to customer records.

The implementation separates incoming records into new and existing records by joining the incoming customer data against the existing Gold dimension using `customer_id`.

For new records:

- A new surrogate key is generated.
- During incremental loads, the key generation starts from the maximum existing `DimCustomerKey` plus one.
- New records receive current creation and update timestamps.

For existing records:

- The existing surrogate key is preserved.
- The original creation date is retained.
- The update date is refreshed when records are processed.
- Updated customer attributes overwrite the previous values.

#### Upsert Logic

The new and existing customer DataFrames are combined using `unionByName()`.

Delta Lake MERGE is then used to implement the upsert operation:

- Matched records are updated.
- Unmatched records are inserted.

The process checks whether the target table exists before deciding whether to create the table or perform a MERGE.

The resulting Gold dimension table is:

`DimCustomers`

### 10.2 Product Dimension – SCD Type 2

The product dimension implements Slowly Changing Dimension Type 2 using **Lakeflow Spark Declarative Pipelines (SDP)** and the AUTO CDC API.

Unlike SCD Type 1, SCD Type 2 preserves historical versions of dimension records when tracked attributes change.

The pipeline was designed using the following components:

**1. DimProducts_stage**

A streaming table that reads incoming product data from `products_silver`.

Data quality expectations are applied to ensure that required fields are present:

- `product_id` must not be NULL.
- `product_name` must not be NULL.

Invalid records are dropped using the configured expectations.

**2. DimProducts_view**

A temporary streaming view that reads from `DimProducts_stage`.

It provides an intermediate source for CDC processing.

**3. DimProducts**

An empty streaming target table is created to receive the CDC results.

**4. AUTO CDC Flow**

The AUTO CDC flow processes changes using:

- Target: `DimProducts`
- Source: `DimProducts_view`
- Primary key: `product_id`
- Sequence column: `updated_at`
- Delete condition: `op = 'DELETE'`
- Excluded columns: `op`, `_rescued_data`
- SCD mode: Type 2

The AUTO CDC API manages historical record versions and applies the appropriate changes to the target dimension.

The resulting Gold dimension table is:

`DimProducts`

### 10.3 Order Fact Table – Star Schema

The order fact table brings together transactional order data and the customer and product dimensions.

The implementation reads:

- `orders_silver` from the Silver Layer.
- `DimCustomers` from the Gold Layer.
- `DimProducts` from the Gold Layer.

#### Dimension Lookups

The following joins are performed:

| Source | Dimension | Join Key |
|---|---|---|
| `orders_silver` | `DimCustomers` | `customer_id` |
| `orders_silver` | `DimProducts` | `product_id` |

Left joins are used to retain the order records while enriching them with the corresponding dimension information.

After the joins:

- The original `customer_id` and `product_id` columns are dropped.
- `DimCustomerKey` and the corresponding product surrogate key are retained.
- The resulting dataset is structured as a fact table.

#### Incremental Upsert

Delta Lake MERGE is used to implement upsert logic for the fact table.

The implementation uses `DeltaTable.forName()` to reference the target Delta table by its registered table name.

The resulting Gold fact table is:

`FactOrders`

---

## 11. End-to-End Pipeline Orchestration

The entire data engineering workflow is orchestrated using Databricks Lakeflow Jobs.

The original Bronze ingestion job was extended into an end-to-end workflow by adding Silver and Gold processing tasks.

### Workflow Execution

The workflow is organized into the following stages:

**Stage 1 – Bronze Ingestion**

The Bronze ingestion workflow dynamically processes the configured source datasets using Auto Loader.

**Stage 2 – Silver Transformations**

Three Silver Layer notebook tasks are configured to run in parallel:

- `Silver_Orders`
- `Silver_Customers`
- `Silver_Products`

All three tasks depend on the completion of the Bronze ingestion task.

**Stage 3 – Gold Dimensions**

The Gold dimension tasks begin after their required Silver Layer dependencies have completed.

- `Gold_Customers` depends on the Silver Customers, Silver Products, and Silver Orders tasks.
- `Gold_Products` depends on the Silver Layer tasks and executes the Lakeflow Spark Declarative Pipeline for SCD Type 2 processing.

**Stage 4 – Gold Fact Table**

The `Gold_Fact` notebook task runs only after both `Gold_Customers` and `Gold_Products` have completed.

This ensures that the required dimensions are available before the fact table is created or updated.

### Orchestration Design

| Task | Dependency |
|---|---|
| Bronze Auto Loader Iteration | Initial task |
| Silver Orders | Bronze ingestion |
| Silver Customers | Bronze ingestion |
| Silver Products | Bronze ingestion |
| Gold Customers | All required Silver tasks |
| Gold Products | Silver Layer tasks |
| Gold Fact | Gold Customers and Gold Products |

Job clusters are configured for the notebook tasks to support scheduled execution.

This dependency-based design improves workflow organization, enables parallel processing where possible, and ensures that downstream transformations execute in the correct order.

---

## 12. Databricks SQL Warehouse and Analytics

After the Gold Layer was implemented, a Databricks SQL Warehouse was created to provide an analytical access layer.

Business analysts can use the SQL Editor to:

- Query the Gold Layer tables.
- Explore the dimensional model.
- Query the `FactOrders` table.
- Combine fact and dimension data for business analysis.
- Save SQL queries for future use.

The Gold Layer provides the curated datasets required for downstream business intelligence and reporting.

Power BI is the intended reporting and visualization layer, using the Gold datasets as the analytical source.

---

## 13. Data Governance and Security

Unity Catalog provides the governance foundation for the project.

The implementation includes:

- Centralized metastore management.
- Catalog and schema organization.
- Storage credentials.
- External locations.
- Managed storage configuration.
- Workspace-level permissions.
- Controlled access to data assets.

The use of an Azure Databricks Access Connector provides managed identity-based access to Azure Data Lake Storage Gen2.

This architecture separates data storage permissions from individual user credentials and supports centralized governance.

---

## 14. Key Engineering Concepts Demonstrated

This project demonstrates practical implementation of several important data engineering concepts:

- Azure cloud infrastructure provisioning.
- Unity Catalog configuration and governance.
- Medallion Architecture.
- Incremental data ingestion.
- Apache Spark Structured Streaming.
- Auto Loader and file discovery.
- Parameter-driven notebook development.
- Lakeflow Jobs and For Each task iteration.
- PySpark transformations and reusable functions.
- Window functions and ranking.
- Delta Lake storage and MERGE operations.
- Surrogate key generation and management.
- Slowly Changing Dimensions Type 1 and Type 2.
- Lakeflow Spark Declarative Pipelines.
- AUTO CDC.
- Star Schema dimensional modelling.
- Job dependency management and parallel execution.
- Databricks SQL analytics.

---

## 15. Suggested Repository Structure

The repository is organized around the different components of the Lakehouse architecture.

```text
Azure-Databricks-Lakehouse-Project/
│
├── README.md
│
├── images/
│   ├── end-to-end-pipeline.png
│   ├── bronze-ingestion-job-1.png
│   └── bronze-ingestion-job-2.png
│
├── notebooks/
│   ├── bronze/
│   │   ├── Parameters.py
│   │   └── Bronze_AutoLoader_Iteration.py
│   │
│   ├── silver/
│   │   ├── Silver_Customers.py
│   │   ├── Silver_Products.py
│   │   ├── Silver_Orders.py
│   │   └── Silver_Regions.py
│   │
│   └── gold/
│       ├── Gold_Customers.py
│       └── Gold_Fact.py
│
├── pipelines/
│   └── gold_products/
│       └── Gold_Products.py
│
├── resources/
│   └── job_and_pipeline_configuration/
│
└── data/
    └── source_datasets/
```

*The structure above is a suggested organization. Adjust the filenames and folders to match the actual files committed to the repository.*

---

## 16. Deployment and Execution Notes

To reproduce this project, the following components are required:

1. An Azure subscription.
2. An Azure Databricks workspace with Unity Catalog enabled.
3. An Azure Data Lake Storage Gen2 account.
4. An Azure Databricks Access Connector with the required storage permissions.
5. Configured Unity Catalog storage credentials and external locations.
6. The source Parquet datasets.
7. A Databricks Git folder connected to the GitHub repository.

### Execution Sequence

1. Configure Azure infrastructure and Unity Catalog.
2. Configure source and target storage locations.
3. Run the Bronze parameter and ingestion notebooks through the Lakeflow Job.
4. Execute the Silver Layer transformation notebooks.
5. Run the Gold Customer dimension notebook.
6. Run the Gold Products Lakeflow Spark Declarative Pipeline.
7. Execute the Gold Fact notebook.
8. Query the Gold Layer using Databricks SQL Warehouse.

**Important:** Storage paths, catalog names, schema names, job configurations, and pipeline settings must be adjusted to match the deployment environment. The project repository should not contain access keys, passwords, or other credentials.

---

## 17. Conclusion

This project demonstrates the implementation of a complete Azure Databricks Lakehouse solution, from raw Parquet ingestion to analytics-ready dimensional models.

It brings together incremental ingestion, reusable PySpark transformations, Delta Lake, SCD Type 1 and Type 2, automated CDC, and end-to-end workflow orchestration.

The result is a structured and governed data platform that transforms operational datasets into reliable, business-ready information.

The project reflects practical data engineering responsibilities involved in designing, developing, and orchestrating a modern Lakehouse solution using the Azure Databricks ecosystem.
