USE ROLE ACCOUNTADMIN;

USE DATABASE SPOTIFY_DATA_ANALYSIS;
USE SCHEMA ANALYSIS;

SELECT * from songs_data;

-- Most Streamed Track:
SELECT track_name, artist_name, streams
FROM songs_data
ORDER BY streams DESC
LIMIT 1;

-- Average Energy and Danceability by Artist:
SELECT artist_name, 
       AVG(energy) AS avg_energy, 
       AVG(danceability) AS avg_danceability
FROM songs_data
GROUP BY artist_name
ORDER BY avg_energy DESC;

-- Tracks Released in the Last Year with High Streams:
SELECT track_name, artist_name, release_date, streams
FROM songs_data
WHERE release_date >= DATEADD(year, -1, CURRENT_DATE())
ORDER BY streams DESC;

-- Top 5 Tracks by BPM:
SELECT track_name, artist_name, bpm
FROM songs_data
ORDER BY bpm DESC
LIMIT 5;

-- Artist with the Most Collaborations:
SELECT artist_name, SUM(artist_count) AS total_collaborations
FROM songs_data
GROUP BY artist_name
ORDER BY total_collaborations DESC
LIMIT 1;

-- Top Artists by Total Streams:
SELECT artist_name, SUM(streams) AS total_streams
FROM songs_data
GROUP BY artist_name
ORDER BY total_streams DESC
LIMIT 5;

-- Top Tracks by Total Streams in Each Month:
WITH monthly_streams AS (
    SELECT 
        track_name,
        artist_name,
        SUM(streams) AS total_streams,
        DATE_TRUNC('month', release_date) AS release_month
    FROM songs_data
    GROUP BY track_name, artist_name, release_month
)
SELECT 
    track_name, 
    artist_name, 
    total_streams,
    release_month
FROM monthly_streams
WHERE (track_name, total_streams) IN (
    SELECT track_name, MAX(total_streams)
    FROM monthly_streams
    GROUP BY release_month
);

-- Cumulative Streams Over Time:
SELECT 
    release_date,
    SUM(streams) OVER (ORDER BY release_date) AS cumulative_streams
FROM songs_data
ORDER BY release_date;

-- Rank Tracks by Streams and Energy:
SELECT 
    track_name,
    artist_name,
    streams,
    energy,
    RANK() OVER (PARTITION BY artist_name ORDER BY streams DESC) AS stream_rank,
    RANK() OVER (PARTITION BY artist_name ORDER BY energy DESC) AS energy_rank
FROM songs_data;

-- Percentage of Streams by Platform:
SELECT 
    artist_name,
    SUM(streams) AS total_streams,
    ROUND(SUM(streams) / (SELECT SUM(streams) FROM songs_data) * 100, 2) AS percentage_of_total_streams
FROM songs_data
GROUP BY artist_name
ORDER BY percentage_of_total_streams DESC;

-- Detecting Outliers in Danceability:
WITH stats AS (
    SELECT 
        AVG(danceability) AS mean,
        STDDEV(danceability) AS stddev
    FROM songs_data
)
SELECT 
    track_name,
    artist_name,
    danceability,
    (danceability - mean) / stddev AS z_score
FROM songs_data, stats
WHERE ABS(z_score) > 2;  -- Assuming z-score threshold of 2 for outlier detection



