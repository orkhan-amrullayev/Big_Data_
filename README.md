# Big_Data_

Introducion
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.
We create an Apache Cassandra database which can create queries on song play data to answer the questions. Then we test database by running queries from Sparkify to create the results.
We create an ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file to model and insert data into Apache Cassandra tables.


Datasets
For this project, you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:

event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv

Project Template
The project template includes one Jupyter Notebook file, in which:

We process the event_datafile_new.csv dataset to create a denormalized dataset
We will model the data tables keeping in mind the queries we need to run
We have been provided queries that we will need to model the data tables for
We will load the data into tables we create in Apache Cassandra and run the queries
Project Steps
Modeling the NoSQL database or Apache Cassandra database

Design tables to answer the queries outlined in the project template
Write Apache Cassandra CREATE KEYSPACE and SET KEYSPACE statements
Develop the CREATE statement for each of the tables to address each question
Load the data with INSERT statement for each of the tables
Include IF NOT EXISTS clauses in the CREATE statements to create tables only if the tables do not already exist. We should include DROP TABLE statement for each table, this way we can run drop and create tables whenever we want to reset the database and test the ETL pipeline
Test by running the proper select statements with the correct WHERE clause
Build ETL Pipeline
Implement the logic in section Part I of the notebook template to iterate through each event file in event_data to process and create a new CSV file in Python
Make necessary edits to Part II of the notebook template to include Apache Cassandra CREATE and INSERT statements to load processed records into relevant tables in the data model
Test by running SELECT statements after running the queries on the database
