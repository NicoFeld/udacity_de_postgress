import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Receives a filepath for a song file, extracts the needed fields and stores the song and artist in the database
    """
    # open song file
    song_df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(song_df[["song_id", "title", "artist_id", "year", "duration"]].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(song_df[["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Receives a filepath for a log file, filters the played songs, transforms the data in order to store the users, the times and the songplays in the database
    """
    # open log file
    log_df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    log_df = log_df.loc[log_df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(log_df['ts'], unit='ms')
    log_df['ts'] = t
    timestamp = t
    hour = t.dt.hour
    day = t.dt.day
    week_of_year = t.dt.weekofyear 
    month = t.dt.month
    year = t.dt.year
    weekday = t.dt.weekday
                                
    # insert time data records
    time_data = [timestamp, 
             hour, 
             day, 
             week_of_year, 
             month, 
             year,
             weekday]
    column_labels = ["timestamp", 
                "hour",
                "day",
                "week of year",
                "month",
                "year",
                "weekday"]
    data_dict = dict(zip(column_labels, time_data))

    time_df = pd.DataFrame(data_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = log_df[["userId", "firstName", "lastName", "gender", "level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in log_df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            continue
        # insert songplay record
        timestamp = row.ts
        level = row.level
        user_id = row.userId
        session_id = row.sessionId
        location = row.location
        user_agent = row.userAgent
        print(timestamp)
        print(level)
        print(user_id)
        print(session_id)
        print(location)
        print(user_agent)
        songplay_data = (songid, timestamp, user_id, artistid, session_id, location, user_agent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Receives a root folder and a handling function.
    Handles every file inside the root folder with the handling function.
    """
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


def main():
    """
    Main function. 
    Connects to the database and starts to process all the files
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()