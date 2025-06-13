import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist varchar(max),
    auth varchar(10),
    firstName varchar(25),
    gender varchar(1),
    itemInSession integer sortkey distkey,
    lastName varchar(25),
    length float,
    level varchar(4),
    location varchar(max),
    method varchar(4),
    page varchar(max),
    registration float,
    sessionId integer,
    song varchar(max),
    status smallint,
    ts bigint,
    user_agent varchar(max),
    user_id integer
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs integer sortkey distkey,
    artist_id varchar(20),
    artist_latitude float,
    artist_longitude float,
    artist_location varchar(max),
    artist_name varchar(max),
    song_id varchar(20),
    title varchar(max),
    duration float,
    year smallint
);
""")

songplay_table_create = ("""
CREATE TABLE songplays (
    songplay_id integer identity(0,1) not null sortkey distkey,
    start_time timestamp not null,
    user_id integer not null,
    level varchar(4) not null,
    song_id varchar(20) not null,
    artist_id varchar(20) not null,
    session_id integer not null,
    location varchar(max),
    user_agent varchar(max) not null
);
""")

user_table_create = ("""
CREATE TABLE users (
    user_id integer not null sortkey distkey,
    first_name varchar(25) not null,
    last_name varchar(25) not null,
    gender varchar(1) not null,
    level varchar(4) not null
);
""")

song_table_create = ("""
CREATE TABLE songs (
    song_id varchar(20) not null sortkey distkey,
    title varchar(max) not null,
    artist_id varchar(20) not null,
    year smallint not null,
    duration float not null
);
""")

artist_table_create = ("""
CREATE TABLE artists (
    artist_id varchar(20) not null sortkey distkey,
    name varchar(max),
    location varchar(max),
    latitude float,
    longitude float
);
""")

time_table_create = ("""
CREATE TABLE time (
    time_id integer identity(0,1) not null sortkey distkey,
    start_time timestamp not null,
    hour smallint not null,
    day smallint not null,
    week smallint not null,
    month smallint not null,
    year smallint not null,
    weekday integer not null
);""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
    FROM 's3://udacity-dend/log_data/'
    credentials 'aws_iam_role={}'
    JSON 's3://udacity-dend/log_json_path.json'
    COMPUPDATE off 
    REGION 'us-west-2'
""".format(config.get('IAM_ROLE', 'ARN')))

staging_songs_copy = ("""
    COPY staging_songs 
    FROM 's3://udacity-dend/song_data/'
    credentials 'aws_iam_role={}'
    JSON 'auto'
    COMPUPDATE off 
    REGION 'us-west-2'
""".format(config.get('IAM_ROLE', 'ARN')))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
    start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT
    TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
    se.user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.user_agent
FROM staging_events se
JOIN staging_songs ss
  ON se.song = ss.title AND se.artist = ss.artist_name
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users (
    user_id, first_name, last_name, gender, level
)
SELECT DISTINCT
    user_id,
    firstName,
    lastName,
    gender,
    level
FROM staging_events
WHERE page = 'NextSong' AND user_id IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (
    song_id, title, artist_id, year, duration
)
SELECT DISTINCT
    song_id,
    title,
    artist_id,
    year,
    duration
FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
    artist_id, name, location, latitude, longitude
)
SELECT DISTINCT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time, hour, day, week, month, year, weekday
)
SELECT DISTINCT
    start_time,
    EXTRACT(hour FROM start_time),
    EXTRACT(day FROM start_time),
    EXTRACT(week FROM start_time),
    EXTRACT(month FROM start_time),
    EXTRACT(year FROM start_time),
    EXTRACT(weekday FROM start_time)
FROM (
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time
    FROM staging_events
    WHERE page = 'NextSong'
) AS time_data;
""")

# ANALYTICS QUERIES

top_artists_query = ("""
SELECT a.name, COUNT(sp.artist_id) AS artist_count
FROM artists a
JOIN songplays sp ON a.artist_id = sp.artist_id
GROUP BY a.name
ORDER BY artist_count DESC
LIMIT 5;
""")

top_genders_query = ("""
SELECT gender, COUNT(gender) AS gender_count
FROM staging_events
WHERE page = 'NextSong'
GROUP BY gender
ORDER BY gender_count DESC;
""")

peak_hours_query = ("""
SELECT hour, COUNT(hour) AS peak_hour_count
FROM time
GROUP BY hour
ORDER BY peak_hour_count DESC
LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

analytics_queries = [
    ("Who are the top 5 most played artists?", top_artists_query),
    ("Which gender listened to more songs?", top_genders_query),
    ("What are the 5 peak listening hours?", peak_hours_query)
]