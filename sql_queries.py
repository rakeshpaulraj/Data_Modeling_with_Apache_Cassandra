# ------------------------------------------------------
# DROP TABLES (If exists already)
# ------------------------------------------------------
music_app_history_session_item_table_drop = "drop table if exists music_app_history_session"
music_app_history_user_session_table_drop = "drop table if exists music_app_history_user_session"
music_app_history_song_table_drop = "drop table if exists music_app_history_song"

# ------------------------------------------------------
# CREATE TABLES
# ------------------------------------------------------

# music_app_history_session table
# partition Key: session_id
# clustering key: item_in_session

music_app_history_session_item_table_create = """
CREATE TABLE IF NOT EXISTS music_app_history_session_item
(
    session_id int, 
    item_in_session int, 
    artist text, song text, 
    length float, 
    primary key(session_id, item_in_session)
)"""

# music_app_history_user_session table
# partition Key: user_id, session_id
# clustering key: item_in_session

music_app_history_user_session_table_create = """
CREATE TABLE IF NOT EXISTS music_app_history_user_session (
    user_id int, 
    session_id int, 
    item_in_session int, 
    artist text, 
    song text, 
    first_name text, 
    last_name text, 
    primary key((user_id, session_id), item_in_session)
)"""

# music_app_history_song table
# partition Key: song
# clustering Key: user_id

music_app_history_song_table_create = """
CREATE TABLE IF NOT EXISTS music_app_history_song (
    song text, 
    user_id int,
    first_name text, 
    last_name text, 
    primary key(song, user_id)
)"""


# ------------------------------------------------------
# INSERT RECORDS
# ------------------------------------------------------

# music_app_history_session_item table
music_app_history_session_item_insert = """
INSERT INTO music_app_history_session_item(session_id, item_in_session, artist, song, length) values(%s, %s, %s, %s, %s)
"""

# music_app_history_user_session table
music_app_history_user_session_insert = """
INSERT INTO music_app_history_user_session(user_id, session_id, item_in_session, artist, song, first_name, last_name) values(%s, %s, %s, %s, %s, %s, %s)
"""

# music_app_history_song table
music_app_history_song_insert = """
INSERT INTO music_app_history_song(song, user_id, first_name, last_name) values(%s, %s, %s, %s)
"""


# ------------------------------------------------------
# QUERY LISTS
# ------------------------------------------------------
create_table_queries = [music_app_history_session_item_table_create, music_app_history_user_session_table_create, music_app_history_song_table_create]
drop_table_queries = [music_app_history_session_item_table_drop, music_app_history_user_session_table_drop, music_app_history_song_table_drop]
