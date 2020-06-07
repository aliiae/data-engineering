song_temp_view = 'songs'
log_temp_view = 'logs'

select_artists = f"""
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM {song_temp_view}
    WHERE artist_id IS NOT NULL
    """

select_songs = f"""
    SELECT DISTINCT song_id,
        title,
        artist_id,
        year,
        duration
    FROM {song_temp_view}
    WHERE song_id IS NOT NULL
"""

select_songplays = f"""
    SELECT logs.start_time,
        logs.userId AS user_id,
        logs.level,
        songs.song_id,
        songs.artist_id,
        logs.sessionId AS session_id,
        logs.location,
        logs.userAgent AS user_agent,
        EXTRACT(
            month
            FROM logs.start_time
        ) AS month,
        EXTRACT(
            year
            FROM logs.start_time
        ) AS year
    FROM {log_temp_view} LEFT OUTER JOIN {song_temp_view} 
        ON song = songs.title
            AND logs.artist = songs.artist_name
            AND logs.length = songs.duration
"""

select_time = f"""
    SELECT DISTINCT start_time,
        EXTRACT(
            hour
            FROM start_time
        ) AS hour,
        EXTRACT(
            day
            FROM start_time
        ) AS day,
        EXTRACT(
            week
            FROM start_time
        ) AS week,
        EXTRACT(
            month
            FROM start_time
        ) AS month,
        EXTRACT(
            year
            FROM start_time
        ) AS year,
        EXTRACT(
            dayofweek
            FROM start_time
        ) AS dayofweek
    FROM {log_temp_view}
    WHERE start_time IS NOT NULL
"""

select_users = f"""
    WITH last_song_play_times AS (
        SELECT userId, MAX(start_time) AS last_song_play
        FROM logs
    GROUP BY userId
    )
    SELECT logs.userId,
        logs.firstName,
        logs.lastName,
        logs.gender,
        logs.level
    FROM {log_temp_view}
    INNER JOIN last_song_play_times t 
        ON logs.userId = t.userId 
        AND logs.start_time = t.last_song_play
"""
