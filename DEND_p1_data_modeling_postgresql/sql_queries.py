# DROP TABLES

drop_table = "DROP TABLE IF EXISTS "
songplay_table_drop = drop_table + "songplay;"
user_table_drop = drop_table + "users;"
song_table_drop = drop_table + "songs;"
artist_table_drop = drop_table + "artists;"
time_table_drop = drop_table + "time;"

# CREATE TABLES

create_table = "CREATE TABLE IF NOT EXISTS "

songplay_table_create = (create_table + """songplay (
        songplay_id SERIAL PRIMARY KEY, 
        start_time timestamp NOT NULL, 
        user_id int NOT NULL, 
        level varchar, 
        song_id varchar, 
        artist_id varchar, 
        session_id int, 
        location varchar, 
        user_agent varchar
    );
""")

user_table_create = (create_table + """users (
        user_id int PRIMARY KEY, 
        first_name varchar, 
        last_name varchar, 
        gender varchar(2), 
        level varchar
    );
""")

song_table_create = (create_table + """songs (
        song_id varchar PRIMARY KEY, 
        title varchar, 
        artist_id varchar, 
        year int, 
        duration double precision
    );
""")

artist_table_create = (create_table + """artists (
        artist_id varchar PRIMARY KEY, 
        name varchar, 
        location varchar, 
        latitude double precision, 
        longitude double precision
    );
""")

time_table_create = (create_table + """time (
        start_time double precision PRIMARY KEY, 
        hour double precision, 
        day varchar, 
        week int, 
        month int, 
        year int, 
        weekday varchar
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) 
    DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, gender = EXCLUDED.gender, level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s) 
    ON CONFLICT (song_id) 
    DO UPDATE SET title = EXCLUDED.title, artist_id = EXCLUDED.artist_id, year = EXCLUDED.year, duration = EXCLUDED.duration;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location)
    VALUES (%s, %s, %s)
    ON CONFLICT (artist_id) DO UPDATE SET name = EXCLUDED.name, location = EXCLUDED.location;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (start_time) DO NOTHING;
""")

# FIND SONGS
song_select = ("""
    SELECT song_id, artists.artist_id 
    FROM songs
    LEFT JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s AND artists.name = %s AND songs.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]