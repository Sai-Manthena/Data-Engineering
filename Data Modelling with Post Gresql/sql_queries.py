# DROP TABLES

songplay_table_drop = "Drop table if exists songplays"
user_table_drop = "Drop table if exists Users"
song_table_drop = "Drop table if exists songs"
artist_table_drop = "Drop table if exists artists"
time_table_drop = "Drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""CREATE TABLE songplays (songplay_id integer PRIMARY KEY,start_time Time NOT NULL,user_id integer NOT NULL,level varchar(20),song_id varchar(256),artist_id varchar(256),session_id INTEGER,location varchar(256),user_agent VARCHAR)""")

user_table_create = ("""CREATE TABLE Users (user_id integer PRIMARY KEY UNIQUE NOT NULL,first_name varchar(100),last_name varchar(100),gender varchar(1),level varchar(20))""")

song_table_create = ("""Create table songs (song_id varchar(256) PRIMARY KEY UNIQUE NOT NULL,Title varchar(256),artist_id varchar(256),year integer not null,duration float not null)""")

artist_table_create = ("""Create table artists(artist_id varchar(256) PRIMARY KEY UNIQUE NOT NULL,Name varchar(256),location varchar(256),latitude float,longitude float)""")

time_table_create = (""" Create table time (start_time Time PRIMARY KEY,hour Integer,Day Varchar,Week Varchar,Month Integer,Year Integer,Weekday varchar)""")

# INSERT RECORDS

user_table_insert = ("""Insert into Users(user_id,first_name,last_name,gender,level) 
                         VALUES (%s, %s, %s, %s, %s)
                         on conflict(user_id)
                         DO UPDATE
                            SET Level = EXCLUDED.Level;""")

song_table_insert = """Insert into songs (song_id,Title,artist_id,year,duration) 
                                         VALUES (%s, %s, %s, %s, %s)
                                         on conflict(song_id)
                                         DO UPDATE
                                           set year = EXCLUDED.Year;"""

artist_table_insert = ("""Insert into artists (artist_id,Name,location,latitude,longitude) VALUES (%s, %s, %s, %s, %s)
                       on conflict (artist_id)
                       DO UPDATE
                          SET Location = EXCLUDED.Location;""")

time_table_insert = (""" Insert into time (start_time,hour,Day,Week,Month,Year,Weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
                         On Conflict(start_time)
                         DO Nothing;
                         """)

songplay_table_insert = ("""INSERT INTO 
                         songplays(songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location, user_agent)
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                         on conflict (songplay_id)
                         DO Nothing;
                         """)

# FIND SONGS

song_select = (""" Select songs.song_id,artists.artist_id
                from 
                songs inner join artists on songs.artist_id = artists.artist_id
                where songs.title=%s AND artists.name=%s AND songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]