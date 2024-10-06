USE ROLE ACCOUNTADMIN;
-- you can create your role with permissions and assign it to user if you do not want to use accountadmin role.

CREATE OR REPLACE DATABASE SPOTIFY_DATA_ANALYSIS;

CREATE OR REPLACE SCHEMA ANALYSIS;

USE DATABASE SPOTIFY_DATA_ANALYSIS;
USE SCHEMA ANALYSIS;

CREATE OR REPLACE STORAGE INTEGRATION my_s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'your_iam_role_arn'
STORAGE_ALLOWED_LOCATIONS = ('your_bucket_uri');

CREATE OR REPLACE STAGE staging_area
URL='your_bucket_uri'
STORAGE_INTEGRATION = my_s3_integration;

CREATE OR REPLACE TABLE songs_data (
    track_name STRING,
    artist_name STRING,
    artist_count INT,
    in_spotify_charts INT,
    streams NUMBER,
    in_apple_charts INT,
    in_shazam_charts INT,
    bpm INT,
    danceability FLOAT,
    valence FLOAT,
    energy FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    speechiness FLOAT,
    release_date DATE
);


COPY INTO songs_data
FROM @staging_area
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*.csv'
ON_ERROR = 'CONTINUE';

SELECT * from songs_data;

CREATE OR REPLACE PIPE spotify_pipe
AUTO_INGEST = TRUE
AS
COPY INTO songs_data
FROM @staging_area
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
PATTERN = '.*.csv'
ON_ERROR = 'CONTINUE';

