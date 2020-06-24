import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'local-Songdata/song_data/*/*/*/*.json' 
    
    # read song data file
    df = spark.read.json(song_data)
    
    # created song view to store data in staguing table
    df.createOrReplaceTempView("stg_song_data")

    # extract columns to create songs table
    songs_table = spark.sql("""
                            SELECT a.song_id, 
                            a.title,
                            a.artist_id,
                            a.year,
                            a.duration
                            FROM stg_song_data a
                            WHERE a.song_id IS NOT NULL
                        """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet(output_data+'songs_table/')

    # extract columns to create artists table
    artists_table = spark.sql("""
                                SELECT DISTINCT b.artist_id, 
                                b.artist_name,
                                b.artist_location,
                                b.artist_latitude,
                                b.artist_longitude
                                FROM stg_song_data b
                                WHERE b.artist_id IS NOT NULL
                            """) 
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists_table/')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log-Testdata'

    # read log data file
    df = spark.read.json(log_data) 
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')
    
    # created staging table for log data
    df.createOrReplaceTempView("Stg_log_data")

    # extract columns for users table    
    users_table = spark.sql("""
                            SELECT DISTINCT 
                            a.userId as user_id, 
                            a.firstName as first_name,
                            a.lastName as last_name,
                            a.gender as gender,
                            a.level as level
                            FROM Stg_log_data a
                            WHERE a.userId IS NOT NULL
                        """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users_table/')

    # create timestamp column from original timestamp column
    #get_timestamp = udf()
    #df = 
    
    # create datetime column from original timestamp column
    #get_datetime = udf()
    #df = 
    
    # extract columns to create time table
    time_table = spark.sql("""
                            SELECT 
                            a.start_time_sub as start_time,
                            hour(a.start_time_sub) as hour,
                            dayofmonth(a.start_time_sub) as day,
                            weekofyear(a.start_time_sub) as week,
                            month(a.start_time_sub) as month,
                            year(a.start_time_sub) as year,
                            dayofweek(a.start_time_sub) as weekday
                            FROM
                            (SELECT to_timestamp(tstamp.ts/1000) as start_time_sub
                            FROM Stg_log_data tstamp
                            WHERE tstamp.ts IS NOT NULL
                            ) a
                        """)
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'time_table/')

    # read in song data to use for songplays table
    #song_df = spark.read.parquet(Output_data/'songs_table/')
    
    # created song view to write SQL Queries
    #song_df_upd.createOrReplaceTempView("song_data_table")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                to_timestamp(a.ts/1000) as start_time,
                                month(to_timestamp(a.ts/1000)) as month,
                                year(to_timestamp(a.ts/1000)) as year,
                                a.userId as user_id,
                                a.level as level,
                                b.song_id as song_id,
                                b.artist_id as artist_id,
                                a.sessionId as session_id,
                                a.location as location,
                                a.userAgent as user_agent
                                FROM Stg_log_data a
                                JOIN stg_song_data b on a.artist = b.artist_name and a.song = b.title
                            """)

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy("year", "month").parquet(output_data+'songplays_table/')


def main():
    spark = create_spark_session()
    input_data = "data/"
    output_data = "Output_data/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
