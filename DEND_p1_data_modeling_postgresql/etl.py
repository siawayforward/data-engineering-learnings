import os
import glob
import json
import psycopg2
import pandas as pd
from sql_queries import *

"""
This function takes a file path for a JSON song file, reads in the data from it, and stores it in the artists and songs dimension tables

Parameters:
-----------
- cur: cursor for the database connection
- filepath: path for JSON file with data to be read

Returns:
--------
None
"""
def process_song_file(cur, filepath):
    # open song file
    df = pd.DataFrame.from_dict(json.load(open(filepath)), orient='index').transpose()

    # insert song record
    line = df.iloc[0]
    song_data = (line['song_id'], line['title'], line['artist_id'], line['year'], line['duration'])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = (line['artist_id'], line['artist_name'], line['artist_location'])
    cur.execute(artist_table_insert, artist_data)

"""
This function takes a file path for a JSON log file, reads in the data from it, and stores it in the time dimension table.
The function also uses data from previously created song and artist table with the time table to create a fact table for all songs

Parameters:
-----------
- cur: cursor for the database connection
- filepath: path for JSON file with data to be read

Returns:
--------
None
"""
def process_log_file(cur, filepath):
    # open log file
    data = [json.loads(line) for line in open(filepath, 'r')]
    df = pd.DataFrame(data)

    # filter by NextSong action
    df = df[df['page'] =='NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = [df.ts, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    #create dictionary of the data with its column headers
    times = dict()
    for unit, col in zip(time_data, column_labels):
        times[col] = unit
    #convert dictionary into data frame
    time_df = pd.DataFrame(times) 

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates().reset_index(drop=True)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


"""
This function is the umbrella to complete the steps of extracting all song data and log files and applying the file processing functions to the data.

Parameters:
-----------
- cur: cursor for the database connection
- conn: database connection
- filepath: path for all JSON files with data to be read
- func: the function for either song or log data to use while processing files in the provided filepath

Returns:
--------
None
"""
def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


"""
Main script execution function
"""
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()