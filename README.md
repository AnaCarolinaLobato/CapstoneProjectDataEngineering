# Data Engineering Capstone Project

The Data Engineering Capstone Project is a comprehensive data engineering project that involves gathering, processing, and modeling data from various sources to create a valuable and insightful data model. The primary dataset used in this project contains information on immigration to the United States, while supplementary datasets cover details on airport codes and U.S. city demographics.

## Project Description

This project aims to demonstrate your data engineering skills by completing the following steps:

### Step 1: Scope the Project and Gather Data

In this initial phase, you will define the scope and objectives of the data engineering project. This involves identifying and gathering the necessary data for your project from at least two sources, ensuring that these sources collectively contain at least two datasets with over 1 million rows. The selection of datasets is crucial as they form the foundation of your analytical model.

End Use Cases:
- Prepare data for analytics to gain insights into immigration patterns, demographic trends, and airport information.
- Create tables for an application's back-end or a source-of-truth database.

### Step 2: Explore and Assess the Data

After gathering the datasets, you will move on to data exploration and assessment. Your goal is to understand the quality and characteristics of the data. You will conduct a thorough data profiling to identify issues such as missing values, duplicate records, outliers, and other data quality concerns. This step is crucial for ensuring the integrity of your data model.

### Step 3: Define the Data Model

The heart of your project is the design of the data model. You have chosen the Star schema as your data modeling approach, simplifying data structures for easier analysis and interpretation. Key components of your data model include:

- `immigration` (Fact Table): This table contains details about immigration events to the United States.
- `demographics` (Dimension Table): Demographics data about U.S. cities provides context for analysis.
- `airports` (Dimension Table): Data on airports, including codes and geographic information.

Your chosen data model enables efficient querying and supports various analyses, making it a valuable resource for decision-makers and analysts.

### Step 4: Run ETL to Model the Data

In this step, you will create data pipelines and the actual data model. Additionally, you will include a data dictionary to describe the fields and their meanings in each table. Quality checks will be run to ensure the success of your pipeline. Integrity constraints will be applied to the relational database, and unit tests will be implemented to validate script functionality.

You will also provide evidence that the ETL has successfully processed the data into the final data model. Importantly, your project should use at least one million rows of data.

### Step 5: Complete Project Write Up

In this final step, you will provide a comprehensive write-up of your project. You will clearly define the project's goals, outline the queries you plan to run on the data, and explain how Spark or Airflow can be incorporated into the project.

- Clearly state the rationale for your choice of tools and technologies.
- Document the steps of the process from data gathering to data model creation.
- Propose how often the data should be updated and why.
- Host your write-up and the final data model in a GitHub repository.

Additionally, you will address the following scenarios:

- If the data was increased by 100x.
- If the pipelines were run on a daily basis by 7 am.
- If the database needed to be accessed by 100+ people.


## Data Quality Checks

To ensure data quality, the following checks have been implemented programmatically in the project:

1. **Schema Validation**: Check if the schema of each table is correct.
2. **Empty Table Check**: Verify if any table is empty.

## Conclusion

The Data Engineering Capstone Project is a comprehensive effort to gather, process, and model data for various use cases. The resulting data model provides valuable insights into immigration patterns, demographics, and airport information. It is designed to support efficient data analysis, visualization, and reporting, aiding in informed decision-making processes. With robust ETL pipelines and data quality checks in place, the project meets the requirements for scalability and reliability.
