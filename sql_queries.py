import configparser


# CONFIG - always us to takes files varaiables and turns it into string values
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS  songplay cascade"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

#staging tables are bringing in to use it from the bucket, to change it and pipe it to use it (events-is logs)
staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events(
                                  artist VARCHAR,
                                  auth TEXT,
                                  first_name TEXT,
                                  gender CHAR(1),
                                  item_session INTEGER,
                                  last_name TEXT,
                                  length NUMERIC,
                                  level TEXT,
                                  location TEXT,
                                  method TEXT,
                                  page TEXT,
                                  registration INTEGER,
                                  session_id INTEGER,
                                  song VARCHAR NOT NULL,
                                  status INTEGER,
                                  ts BIGINT,
                                  user_agent TEXT,
                                  user_id INTEGER)
                                  """)

staging_songs_table_create  = ("""CREATE  TABLE IF NOT EXISTS staging_songs(
                                  artist_id VARCHAR NOT NULL,
                                  latitude NUMERIC,
                                  location VARCHAR,
                                  longitude NUMERIC,
                                  artist_name VARCHAR(MAX),
                                  duration NUMERIC NOT NULL,
                                  num_songs INTEGER,
                                  song_id VARCHAR NOT NULL,
                                  title TEXT,
                                  year INTEGER)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplay(
                            songplay_id INTEGER IDENTITY(1,1) NOT NULL PRIMARY KEY,
                            start_time TIMESTAMP NOT NULL,
                            user_id INTEGER,
                            level TEXT,
                            song_id VARCHAR NOT NULL,
                            artist_id VARCHAR NOT NULL,
                            session_id INTEGER,
                            location VARCHAR,
                            user_agent TEXT
                            )
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        gender CHAR(1),
                        level TEXT)
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS song(
                            song_id VARCHAR NOT NULL PRIMARY KEY,
                            title TEXT,
                            artist_id VARCHAR NOT NULL,
                            year INTEGER,
                            duration NUMERIC)
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artist(
                            artist_id VARCHAR NOT NULL PRIMARY KEY,
                            name VARCHAR,
                            location TEXT, 
                            latitude NUMERIC,
                            longitude NUMERIC)
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
                            start_time TIMESTAMP NOT NULL PRIMARY KEY,
                            hour INTEGER,
                            day INTEGER,
                            week INTEGER,
                            month INTEGER,
                            year INTEGER,
                            weekday INTEGER
)
""")

# STAGING TABLES

staging_events_copy = (f"""copy staging_events
                        from {config['S3']['LOG_DATA']}
                        credentials 'aws_iam_role={config['IAM_ROLE']['ARN']}'
                        region 'us-west-2'
                        compupdate off
                        statupdate off
                        json {config['S3']['LOG_JSONPATH']};
                        """)


staging_songs_copy = (f"""copy staging_songs
                        from {config['S3']['SONG_DATA']}
                        credentials 'aws_iam_role={config['IAM_ROLE']['ARN']}'
                        region 'us-west-2'
                        compupdate off
                        statupdate off
                        json 'auto';
                        """)

# staging_events_copy = ("""copy staging_events
#                         from {}
#                         credentials 'aws_iam_role={}'
#                         region 'us-west-2'
#                         compupdate off
#                         statupdate off
#                         json {};
#                         """).format(config['S3']['LOG_DATA'],
#                                     config['IAM_ROLE']['ARN'],
#                                     config['S3']['LOG_JSONPATH'])
# staging_songs_copy = ("""copy staging_songs
#                         from {}
#                         credentials 'aws_iam_role={}'
#                         region 'us-west-2'
#                         compupdate off
#                         statupdate off
#                         json 'auto';
#                         """).format(config['S3']['SONG_DATA'],
#                                     config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay(songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
                                SELECT se.ts AS start_time,
                                        se.user_id,
                                        ss.level,
                                        ss.song_id,
                                        ss.artist_id,
                                        se.session_id,
                                        se.location,
                                        ss.user_agent
                                FROM staging_events AS se
                                JOIN staging_songs ss ON (se.artist = ss.artist_id)

""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                                SELECT DISTINCT(user_id),
                                        first_name,
                                        last_name,
                                        gender,
                                        level
                                FROM staging_events
""")

song_table_insert = ("""INSERT INTO song(song_id, title, artist_id, year, duration)
                            SELECT DISTINCT(song_id),
                            title,
                            artist_id,
                            year, 
                            duration
                            FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artist(artist_id, name, location, latitude, longitude)
                            SELECT DISTINCT(artist_id),
                            artist_name AS name,
                            location,
                            latitude,
                            longitude
                            FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, month, year, week, weekday)
                            SELECT  ts AS start_time,
                                    EXTRACT(hour FROM ts) AS hour,
                                    EXTRACT(day FROM ts) AS day,
                                    EXTRACT(month FROM ts) AS month,
                                    EXTRACT(year FROM ts) AS year,
                                    EXTRACT(week FROM ts) AS week,
                                    EXTRACT(weekday FROM ts) AS weekday
                            FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, song_table_drop, user_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
