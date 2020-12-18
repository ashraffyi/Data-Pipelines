class SqlQueries:
    
    #Drop Tables
    table_drop= """
     DROP TABLE IF EXISTS {}
     """


    #Create Tables
    staging_events_table_create= ("""
     CREATE TABLE IF NOT EXISTS staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstname VARCHAR,
            gender CHAR(1),
            iteminsession INT,
            lastname VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration BIGINT,
            sessionid INT,
            song VARCHAR ,
            status INT,
            ts BIGINT SORTKEY,
            useragent VARCHAR,
            userid INT
        )
    """)
    
    staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id VARCHAR,
            artist_location VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_name VARCHAR,
            duration FLOAT,
            num_songs INT,
            song_id VARCHAR,
            title VARCHAR,
            year INT
        )
    """)

    songplays_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR PRIMARY KEY SORTKEY ,
            start_time TIMESTAMP,
            user_id VARCHAR,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INT,
            location VARCHAR,
            user_agent VARCHAR
        )
        DISTSTYLE AUTO
    """)

    users_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
            user_id VARCHAR PRIMARY KEY SORTKEY,
            first_name VARCHAR,
            last_name VARCHAR,
            gender CHAR(1),
            level VARCHAR
        )
        DISTSTYLE AUTO
    """)

    songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY SORTKEY,
            title VARCHAR,
            artist_id VARCHAR,
            year INT,
            duration INT
        )
        DISTSTYLE AUTO
    """)

    artists_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY SORTKEY,
            name VARCHAR,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        )   
        DISTSTYLE AUTO
    """)

    time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY SORTKEY,
            hour INT,
            day INT,
            week INT,
            month INT,
            year INT,
            weekday INT
        )
        DISTSTYLE AUTO
    """)
    
    
    
    
    
    #Insert
    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.start_time) songplay_id,
                events.start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    copy_sql = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    {}
    """