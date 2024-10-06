import pandas as pd

def transform_csv(file_path):
    df = pd.read_csv(file_path)
    df.columns = [col.lower() for col in df.columns]
    df = df.drop_duplicates()
    df = df.dropna()
    df = df.drop(columns=['cover_url', 'key', 'mode', 'in_deezer_playlists', 'in_deezer_charts', 'in_spotify_playlists', 'in_apple_playlists'])

    # Convert percentage columns to decimal
    percentage_cols = ['danceability_%', 'valence_%', 'energy_%', 'acousticness_%', 
                    'instrumentalness_%', 'liveness_%', 'speechiness_%']

    for col in percentage_cols:
        df[col] = df[col] / 100.0

    df['release_date'] = df.apply(
    lambda row: pd.to_datetime(
        f"{int(row['released_year'])}-{int(row['released_month'])}-{int(row['released_day'])}",
        errors='coerce'
    ), 
    axis=1
    )

    df = df.drop(columns=['released_year', 'released_month', 'released_day'])
    
    return df
