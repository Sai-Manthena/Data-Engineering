# Project: Data Warehouse.
Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Description
In this project using Spark and Datalake knowledge,we will build an ETL pipeline for a data lake hosted on S3. To complete the project, we will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. we will deploy this Spark process on a cluster using AWS. 
 

# Project Datasets

Source data: 
  1. Song Dataset: a subset of million song dataset containing.Each file is in JSON format and contains metadata about a song and the artist of that song
     s3 link : s3://udacity-dend/song_data
  2. log Dataset: log files in JSON format generated by event simulator based on Song dataset.
     S3 link : s3://udacity-dend/log_data
  

# Schema for Song play analysis

Schema Design: The Goal of the design is to organize the data to avoid duplication of fields and repeating data, and to ensure quality of the data to perform song play analysis.
  
  Fact table contains all the metrics data and using primary_key we can make joins to dimension tables to get necessary info. 
  
  
  Fact Table
   1. songplays: records in log data associated with song plays i.e. records with page NextSong
   - songplay_id(Primary Key), start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
  Dimension Tables
   1. users:users in the app
   - user_id(Primary_Key), first_name, last_name, gender, level
   2. songs:songs in music database
   - song_id(Primary_Key), title, artist_id, year, duration
   3. artists:artists in music database
   - artist_id(Primary_Key), name, location, latitude, longitude
   4. time:timestamps of records in songplays broken down into specific units
   - start_time, hour, day, week, month, year, weekday
   
  For EX: To Identify how long each user is spending on the website. 
  
Select 
  b.user_id,
  b.first_name,
  b.last_name,
  sum(a.session_id)
  from 
  fact_songplay a inner join 
  dim_Users b on a.user_id = b.user_id 
  group by 
   b.user_id, b.first_name,b.last_name ;
   
# Project Template
  
dl.cfg : contains your AWS credentials 
etl-Local.py :reads data from local input folder containing smalled data set, processes that data using Spark, and writes them back to Output folder.
etl.py : reads data from S3, processes that data using Spark, and writes them back to S3
README.md is where you'll provide discussion on your process and decisions for this ETL pipeline   

# step by step process to complete project.

Build ETL Pipeline
 1. Load the AWS credentials in dl.cfg file.
 2. Implement the logic in etl.py which  reads JSON files from S3, processes that data using Spark and create analytical tables(Fact & Dimension) and writes them back to S3.
 3. Run the script etl script in Terminal by "python etl.py"  

