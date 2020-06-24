import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES

staging_events_table_drop = "Drop table if Exists staging_events "
staging_songs_table_drop = "Drop table if Exists staging_songs "
songplay_table_drop = "Drop table if Exists fact_songplay"
user_table_drop = "Drop table if Exists dim_users"
song_table_drop = "Drop table if Exists dim_songs"
artist_table_drop = "Drop table if Exists dim_artists"
time_table_drop = "Drop table if Exists dim_time"

# CREATE TABLES

staging_events_table_create= (""" Create table if not exists Staging_Events 
(artist varchar,
Auth varchar,
first_name varchar,
gender varchar,
IteminSession Integer,
last_name varchar,
length float,
level varchar,
location varchar,
method varchar,
page varchar,
registration BIGINT,
sessionid Integer,
Song varchar,
status integer,
ts timestamp,
user_agent varchar,
User_id Integer
)
""")

staging_songs_table_create = (""" Create table if not exists staging_songs
(
num_songs integer,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration float,
Year integer
)
""")

songplay_table_create = (""" Create table if not exists fact_songplay
(songplay_id integer IDENTITY(0,1) PRIMARY KEY,
start_time Timestamp,
user_id integer distkey,
level varchar(20),
song_id varchar(256),
artist_id varchar(256),
session_id INTEGER,
location varchar(256),
user_agent VARCHAR)
""")


user_table_create = ("""CREATE TABLE dim_Users
(user_id integer PRIMARY KEY distkey,
first_name varchar(100),
last_name varchar(100),
gender varchar(1),
level varchar(20))
""")

song_table_create = ("""Create table dim_songs
(song_id varchar(256) PRIMARY KEY,
Title varchar(256),
artist_id varchar(256),
year integer not null,
duration float not null)
diststyle all ;
""")

artist_table_create = ("""Create table dim_artists
(artist_id varchar(256) PRIMARY KEY ,
artist_Name varchar(256),
location varchar(256),
latitude float,
longitude float)
diststyle all ;
""")

time_table_create = (""" Create table dim_time
(start_time Timestamp PRIMARY KEY sortkey,
hour Integer,
Day Varchar,
Week Varchar,
Month Integer,
Year Integer,
Weekday varchar)
diststyle all ;
""")

# STAGING TABLES
staging_events_copy = ("""
                       Copy Staging_Events from 's3://udacity-dend/log_data'
                       Credentials 'aws_iam_role={}'
                       region 'us-west-2'
                       TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL TIMEFORMAT as 'epochmillisecs'
                       FORMAT AS JSON 's3://udacity-dend/log_json_path.json';
                       """).format('arn:aws:iam::835243883054:role/dwhRole')

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data'
    CREDENTIALS 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto' 
    BLANKSASNULL EMPTYASNULL TRUNCATECOLUMNS;
""").format('arn:aws:iam::835243883054:role/dwhRole')

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO fact_songplay(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT to_timestamp(to_char(a.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                a.user_id,
                a.level as level,
                b.song_id,
                b.artist_id,
                a.sessionid as session_id,
                a.location,
                a.user_agent
FROM staging_events a
JOIN staging_songs b ON a.song = b.title AND a.artist = b.artist_name;
""")

user_table_insert = ("""
-- Create a staging table and populate it with rows from source staging_events table
create temp table dim_users_stg as 
SELECT DISTINCT a.user_id,
                a.first_name,
                a.last_name,
                a.gender,
                a.level
FROM staging_events a
where a.user_id is not null ;

begin transaction;
-- Update the target table using an inner join with the staging table
update dim_Users
set level = dim_users_stg.level
from dim_users_stg
where dim_users.user_id = dim_users_stg.user_id
and dim_users.level != dim_users_stg.level;

-- Delete matching rows from the staging table 
-- using an inner join with the target table

delete from dim_users_stg
using dim_users
where dim_users.user_id = dim_users_stg.user_id;

-- Insert the remaining rows from the staging table into the target table

insert into dim_users
select * from dim_users_stg;

-- End transaction and commit
end transaction;

-- Drop the staging table
drop table dim_users_stg;
""")

song_table_insert = ("""INSERT INTO dim_songs(song_id, Title, artist_id, year, duration)
SELECT DISTINCT a.song_id,
                a.title,
                a.artist_id,
                a.year,
                a.duration
FROM staging_songs a
WHERE a.song_id IS NOT NULL
""")

artist_table_insert = ("""INSERT INTO dim_artists(artist_id, artist_name, location, latitude, longitude)
SELECT DISTINCT a.artist_id,
                a.artist_name,
                a.artist_location as location,
                a.artist_latitude as latitude,
                a.artist_longitude as longitude
FROM staging_songs a
where a.artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
SELECT distinct a.ts,
                EXTRACT(hour from a.ts),
                EXTRACT(day from a.ts),
                EXTRACT(week from a.ts),
                EXTRACT(month from a.ts),
                EXTRACT(year from a.ts),
                EXTRACT(weekday from a.ts)
FROM staging_events a
WHERE ts IS NOT NULL;
""")

# QUERY LISTS

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
