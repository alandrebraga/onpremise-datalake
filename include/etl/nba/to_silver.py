from airflow.decorators import task
import polars as pl
from include.utils import utils
from io import BytesIO

@task()
def bronze_to_silver_nba_dataset():

    client = utils.get_minio_client()

    raw_nba = client.get_object("nba", "bronze/nba_raw.csv")

    df_nba = pl.read_csv(raw_nba)

    columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP", "MIN", "FGM"]
    df_nba = df_nba.select(columns)

    renamed_columns = {
        "year": "Year",
        "Season_type": "season_type",
        "PLAYER_ID": "player_id",
        "RANK": "rank",
        "PLAYER": "player_name",
        "TEAM_ID": "team_id",
        "TEAM": "team_abbreviation",
        "GP": "games_played",
        "MIN": "minutes",
        "FGM": "field_goals_made"
    }

    df_nba = df_nba.rename(renamed_columns)

    df_nba = df_nba.with_columns([
        pl.col("player_id").cast(pl.Int32),
        pl.col("rank").cast(pl.Int32),
        pl.col("games_played").cast(pl.Int32),
        pl.col("minutes").cast(pl.Int32),
        pl.col("field_goals_made").cast(pl.Int32)
    ])

    data = BytesIO()
    df_nba.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)

    client.put_object(
        bucket_name="nba",
        object_name="silver/nba.parquet",
        length=length_bytes,
        data=data
    )

    print(f"File 'nba.parquet' uploaded successfully")


@task()
def bronze_to_silver_playoff_dataset():
    client = utils.get_minio_client()

    raw_playoffs = client.get_object("nba", "bronze/raw_playoffs.csv")

    df_playoffs = pl.read_csv(raw_playoffs)
    columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP"]
    df_playoffs = df_playoffs.select(columns)

    renamed_columns = {
        "year": "Year",
        "Season_type": "season_type",
        "PLAYER_ID": "player_id",
        "RANK": "rank",
        "PLAYER": "player_name",
        "TEAM_ID": "team_id",
        "TEAM": "team_abbreviation",
        "GP": "games_played"
    }

    df_playoffs = df_playoffs.rename(renamed_columns)

    df_playoffs = df_playoffs.with_columns([
        pl.col("player_id").cast(pl.Int32),
        pl.col("rank").cast(pl.Int32),
        pl.col("games_played").cast(pl.Int32)
    ])

    data = BytesIO()
    df_playoffs.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)

    client.put_object(
        bucket_name="nba",
        object_name="silver/playoffs.parquet",
        length=length_bytes,
        data=data
    )
    print(f"File 'playoffs.parquet' uploaded successfully")


@task()
def bronze_to_silver_regular_season():
    client = utils.get_minio_client()

    raw_regular_season = client.get_object("nba", "bronze/raw_regular_season.csv")

    df_regular_season = pl.read_csv(raw_regular_season)
    columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP"]
    df_regular_season = df_regular_season.select(columns)

    renamed_columns = {
        "year": "Year",
        "Season_type": "season_type",
        "PLAYER_ID": "player_id",
        "RANK": "rank",
        "PLAYER": "player_name",
        "TEAM_ID": "team_id",
        "TEAM": "team_abbreviation",
        "GP": "games_played"
    }

    df_regular_season = df_regular_season.rename(renamed_columns)

    df_regular_season = df_regular_season.with_columns([
        pl.col("player_id").cast(pl.Int32),
        pl.col("rank").cast(pl.Int32),
        pl.col("games_played").cast(pl.Int32)
    ])

    data = BytesIO()
    df_regular_season.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)

    client.put_object(
        bucket_name="nba",
        object_name="silver/regular_season.parquet",
        length=length_bytes,
        data=data
    )

    print(f"File 'regular_season.parquet' uploaded successfully")