<img width="1301" height="1511" alt="BDA_Architecture" src="https://github.com/user-attachments/assets/11287fda-b75a-4061-b42e-ffca490773c5" />

The BDA architecture is composed by 5 components :
1) Staging Area: This is the centralized component where data coming from different sources are queued before the ingestion process into the Data Lakehouse. It’s It is structured into four sub-areas:
- Incoming Area : The entry point of the component, where files are initially collected for validation. After the check, files are moved either to the Approved Area (if validation is successful) or to the Rejected Area (if validation fails).
- Approved Area: Contains the validated files that are ready for ingestion. These files are copied into the distributed file system and subsequently moved to the Ingested Area.
- Rejected Area: Stores files with structural errors that must be re-acquired or corrected before reprocessing.
- Ingested Area: The final sub-area, which contains files that have already been ingested and are awaiting purge.
The Staging area is implemented in Hadoop (HDFS)

------------------------------------------------------------

2) Storage Area: This is the central component where data is processed and stored. It is divided into two sub-components: the Current Area and the Historical Area. Since data from the Storage Area must be consumed by business users and other components, specific business logics need to be applied.
The application of business logic to processed data, and their subsequent movement across components, is referred to as the Retention Process. The Current Area is where ingested data is initially stored; it contains data that must be processed and pass through the Retention Process. 
Once the process is completed, data is moved from the Current Area to the Historical Area. The Historical Area contains retentioned data, whose main purpose is to serve as the “single source of truth” for large-scale analysis and historical storage.
Both the Current Area and the Historical Area are organized into three layers:
Bronze Layer: The first layer, which contains raw files.
Silver Layer: The intermediate layer, which contains cleaned, validated, and lightly processed data.
Gold Layer: The final layer, which contains aggregated and transformed data, ready to be used as input datasets for ML models.
Data in each layer is stored in .parquet format, which allows queries to be executed directly on the Storage using Apache Spark, with the SQL module if needed. The stored files are not only archived but can also be actively leveraged for large-scale analysis. 
The Storage Area is implemented in Hadoop (HDFS).

-----------------------------------------------------------

3) Data Warehouse: This component provides the systems needed to perform both small-scale and large-scale analyses using SQL. It is composed of an OLTP system (PostgreSQL DB) for small-scale queries and an OLAP system (Apache Hive) for massive analytical workloads.
- The OLTP System is PostgreSQL, the data model is based on a star schema, where the central Fact Table is the Trip Event. This table contains data aggregated from the Silver Layer, representing all events recorded for each trip. The Fact Table is connected to three Dimensional Tables: Source_File_Dimension, Contract_Dimension, and Province_Dimension. Additionally, to enable deeper and more granular analyses, the Fact_Trip table is linked to the detailed event tables (from the Bronze layer), where each event type is stored in a separate table. The relationship is maintained through the common foreign key Trip_ID.
- The OLAP System is Apache Hive. It is a data warehouse infrastructure built on top of the Hadoop ecosystem, designed to facilitate the querying and analysis of large-scale datasets stored in distributed environments. Hive addresses the need to provide a SQL-like abstraction (HiveQL) for processing data in Hadoop Distributed File System (HDFS). It allows analysts and business users to perform data exploration and analytical tasks using a declarative query language similar to SQL.
One of the fundamental concepts in Hive is the adoption of a schema-on-read approach. Unlike traditional relational databases, where the schema must be defined before the data is ingested (schema-on-write), Hive allows raw data to be loaded directly into the system. The schema is applied at query time, providing flexibility when dealing with heterogeneous and semi-structured data. This makes Hive particularly suitable for Big Data environments. 
From an analytical perspective, Hive supports partitioning, bucketing, and indexing as mechanisms to optimize query performance, especially when dealing with terabytes or petabytes of information. Furthermore, integration with columnar file formats such as Parquet enhances storage efficiency and query execution times. Apache Hive is considered a de facto standard for Big Data warehousing.  
Its ecosystem has evolved to support not only batch-oriented workloads but also interactive queries and integration with modern Big Data processing engines. For this reason, Hive plays a critical role in bridging the gap between traditional Business Intelligence systems and the scalability of distributed computing frameworks.


The architecture of Hive is composed of several key components:
    • HiveQL (Query Language): An SQL-inspired language that allows users to define schemas, query large datasets, and perform aggregations. Queries are internally translated into execution plans based on MapReduce (Apache Spark in our case).
    • Metastore: A central repository that stores metadata about tables, partitions, and schemas, enabling the system to interpret the logical structure of data.
    • Execution Engine: Responsible for transforming HiveQL queries into directed acyclic graphs of tasks that are executed on the underlying distributed processing framework.
    • Storage Layer: Hive is storage-agnostic, meaning that data can reside not only in HDFS but also in cloud storage systems (e.g., Amazon S3, Azure Blob Storage) or other compatible file systems.
In this project, a Hive database was created by externalizing the bronze and silver historical files into tables. Each table points to its corresponding HDFS folder, so when new data is appended to the historical directories, the Hive tables are automatically updated in real time. All tables use the ingestion_date as the partitioning parameter.

------------------------------------------------------

4) Access : In order to obtain a clearer and more user-friendly graphical representation of the data collected and processed in the Data Lakehouse, business users and managers should rely on real-time, interactive dashboards.
The Access component is a main component for the BI process. It contains Apache Superset, an open-source data exploration and business intelligence (BI) platform designed to provide interactive data visualization and advanced analytics at scale. 
Superset is designed to operate in a highly scalable environment, supporting both lightweight analytical queries on relational databases and large-scale data exploration on distributed data warehouses such as Apache Hive, Apache Druid, Presto, and modern cloud-native warehouses.
One of Superset’s distinguishing features is its emphasis on extensibility and interoperability. The platform supports a broad set of visualization plugins, customizable dashboards, and integration with authentication protocols such as LDAP, OAuth, and OpenID Connect. 
Superset is fully compatible with modern big data architectures, allowing seamless interaction with data stored in Data Lakes, Data Warehouses, and Lakehouses.

------------------------------------------------------

5) ML Models: The primary consumers of telematics data are the machine learning models, which are designed to predict the likelihood of customers filing claims, as well as the potential severity and type of those claims, particularly at the time of policy renewal.
Three models have been implemented, all developed in Python using the PyTorch library and trained on CUDA-enabled resources (NVIDIA RTX 4060):
RCA Frequency Model: A binary classification model in which the label function indicates whether or not the customer will file fault claims.
Incurred Model: A regression model that estimates the expected severity of claims, i.e., the projected cost for the company.
Claim Type Model: A multi-label classification model that predicts whether future claims will involve bodily injury, property damage, or both.
All three models are based on deep learning architectures with residual blocks (skip connections).
While the model structures and datasets vary for each use case, they all share the same initial set of features, imported from the Gold Layer during the preprocessing phase.
The variables used across the models are independent. A more detailed discussion of each model will be provided in the following sections.

-----------------------------------------------------

There are secondary components that are not part of the data pipeline itself but are essential to ensure a complete and stable architecture:
- Backup: Although the computer cluster is fault-tolerant, it is still advisable to store a copy of the information in a geographically distant location. This is because critical events could disable all cluster nodes for an extended period of time.
          For this project, AWS S3 Bucket has been selected as the backup service, since it is well-integrated with Python through the Boto3 library.

- Orchestrator : The entire architecture operates through a sequence of Python scripts. These scripts must be scheduled, and their execution must follow a workflow that respects the dependencies between them. In larger architectures, it is recommended to adopt workflow orchestration tools such as Apache Oozie or Apache Airflow. However, in this project, Crontab, a Linux-native scheduler, has been used.
The workflow of the architecture is divided into three independent sequences:
Staging and Ingestion: This sequence includes all scripts responsible for processing and moving files from the Staging Area to the Gold Layer. It is executed once per day.
Retention, Backup, and ML Retraining: This sequence encompasses all operations performed when data in the Current Area is moved to the Historical Area. It also includes backup procedures and the retraining of machine learning models. It is executed once per week.
Staging Area Purging: This sequence handles the removal of files that have already been ingested and stored in the Historical Area. It is executed once per month.

- Logs: Logs are essential in every system to understand both what is currently happening and what has already happened. In this architecture, logs are produced by multiple components : 
Hadoop, Hive, and Superset : Each of them stores its logs in a dedicated folder inside their respective home directory.
Python, Spark: Their logs are externalized and centralized in a directory system. For each script, a dedicated directory is used to collect its Python and Spark logs.
For every script execution, a new set of log files is not created; instead, the system appends the new logs to the existing log files.

- Localhost (provider simulator): In this project, the data sources are simulated using a localhost directory. It contains all the original raw data used to train the ML models, split into multiple raw files.
- To simulate the progressive daily upload of data, the localhost is divided into two areas: Available and Not Available.
- The architecture collects files stored in the Available area. A Python script is responsible for moving random raw files from the Not Available area to the Available area, and it is always executed before the other scripts.
