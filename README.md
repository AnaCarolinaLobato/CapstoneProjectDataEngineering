Data Engineering Capstone Project
The Data Engineering Capstone Project represents a comprehensive data engineering initiative, involving the collection, processing, and modeling of data from diverse sources to construct a valuable and insightful data model. The primary dataset for this project encompasses information on immigration to the United States, supplemented by datasets covering airport codes and U.S. city demographics.

Project Description
This project aims to showcase advanced data engineering skills through the following key steps:

Step 1: Scope the Project and Gather Data
In the initial phase, the project scope and objectives are defined. Data is gathered from at least two sources, ensuring a collective dataset of over 1 million rows. The chosen datasets serve as the foundation for an analytical model with applications in understanding immigration patterns, demographic trends, and airport information.

Step 2: Explore and Assess the Data
After gathering datasets, a thorough exploration and assessment phase begins. Data profiling is conducted to understand data quality and characteristics, identifying issues like missing values, duplicate records, and outliers. This step is pivotal for ensuring the integrity of the subsequent data model.

Step 3: Define the Data Model
The heart of the project lies in designing the data model. The Star schema is employed, providing a simplified data structure for easier analysis and interpretation. Key components of the data model include:

immigration (Fact Table): Details about immigration events to the United States.
demographics (Dimension Table): U.S. city demographics providing contextual information.
airports (Dimension Table): Data on airports, including codes and geographic details.
This data model supports efficient querying and diverse analyses, offering valuable insights to decision-makers and analysts.

Step 4: Run ETL to Model the Data
This step involves creating data pipelines, implementing the data model, and incorporating a data dictionary. Quality checks ensure the success of the pipeline, applying integrity constraints to the relational database and implementing unit tests to validate script functionality. Evidence is provided that the ETL has successfully processed the data into the final data model, with the project utilizing at least one million rows of data.

Step 5: Complete Project Write Up
The final step includes a comprehensive write-up outlining the project's goals, planned queries, and the incorporation of Spark or Airflow. The rationale for tool and technology choices is clearly stated, documenting the entire process from data gathering to data model creation. Considerations for data updates, hosting the write-up and final data model on GitHub, and addressing scalability scenarios are also covered.

Additionally, the project addresses the following scenarios:

If the data was increased by 100x
Spark's powerful processing engine is capable of handling a 100x increase in data. Processing data in partitions and utilizing AWS EMR for large-scale data analytics ensures efficient data handling.

If the pipelines were run on a daily basis by 7 am
Scheduling the monthly DAG to run before 7 am ensures timely updates, preventing downstream tasks from failing or loading empty data.

If the database needed to be accessed by 100+ people
Implementing access management with user groups and role-based permissions ensures secure and controlled database access, making onboarding new users a streamlined process.

Data Quality Checks
To ensure data quality, the following checks are implemented programmatically:

Schema Validation: Checking if the schema of each table is correct.
Empty Table Check: Verifying if any table is empty.
Conclusion
The Data Engineering Capstone Project is a comprehensive endeavor to gather, process, and model data for various use cases. The resulting data model offers valuable insights into immigration patterns, demographics, and airport information. Designed to support efficient data analysis, visualization, and reporting, the project stands as a robust solution for informed decision-making. With well-established ETL pipelines and data quality checks, the project meets the requirements for scalability and reliability.
