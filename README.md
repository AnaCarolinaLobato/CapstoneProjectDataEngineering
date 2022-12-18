# Data Engineering Capstone Project
The purpose of the data engineering capstone project is to work with datasets to complete the project. The main dataset will include data on immigration to the United States, and supplementary datasets will include data on airport codes, U.S. city demographics.

# Steps of the Project

## Step 1: Scope the Project and Gather Data
In this step will identify and gather the data will be using for the project (at least two sources and more than 1 million rows).

## Step 2: Explore and Assess the Data
Explore the data to identify data quality issues, like missing values, duplicate data, etc.

## Step 3: Define the Data Model
The Star schema model will be using because are simple e clear structure of data and can take easier analysis and understand results.
Table	type
immigration	fact table
demographics	dimension table
airports	dimension table

The steps necessary to pipeline the data into the chosen data model
-run the ETL enter your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY into keys.cfg file. 
-run the etl.py scrip to trigger spark job that processes and transforms the data into a combination of facts and dimension tables. 
-Check for the output in your s3 data lake to confirm successful load.

## Step 4: Run etl.py to Model the Data
Create the data pipelines and the data model
Include a data dictionary
Run data quality checks to ensure the pipeline ran as expected
Integrity constraints on the relational database (e.g., unique key, data type, etc.)
Unit tests for the scripts to ensure they are doing the right thing

## Step 5:description of how would be approach the problem differently under the following scenarios:

##If the data was increased by 100x.
If data was increased by 100x Spark's powerful processing engine would be able to handle it. We could also process the data in partitions so as to work within memory limits. Also, in place of running the Spark job on single user machined we would host Spark in AWS EMR which allows running peta-byte scale data analytics faster

## If the pipelines were run on a daily basis by 7am.
As the data immigration is updated on a monthly basis, I would schedule the monthly DAG to run before 7 AM, so give or take 2 hours before to append new data in the tables so that the downstream dependent task does not fail or load empty.

## If the database needed to be accessed by 100+ people.
Implement access management by adding users to groups with different data needs,then grant read permissions to tables frequently used by the different groups. Hence based on group/role all I would need to do when a new analyst/employee joins the company is add them to any of the predefined group and they inherit all access rights relating to the group.
