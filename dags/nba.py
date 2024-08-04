
from airflow.decorators import dag, task
from pendulum import datetime
from airflow.models.baseoperator import chain
from include.utils import utils
import pandas as pd

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb", "retries": 3},
    tags=["nbadag"],
)
def nba():

    @task()
    def create_nba_bucket():
        client = utils.get_minio_client()

        if not client.bucket_exists("nba"):
            client.make_bucket("nba")
            print(f"Bucket 'nba' created successfully")

    @task()
    def upload_nba_raw_dataset():
        client = client = utils.get_minio_client()

        client.fput_object("nba", "bronze/nba_raw.csv", "include/datasets/nba.csv")
        print(f"File 'nba_raw.csv' uploaded successfully")

    @task()
    def upload_playoff_dataset():
        client = client = utils.get_minio_client()

        client.fput_object("nba", "bronze/raw_playoffs.csv", "include/datasets/Playoffs.csv")
        print(f"File 'nba_playoff.csv' uploaded successfully")

    @task()
    def upload_regular_season_dataset():
        client = client = utils.get_minio_client()

        client.fput_object("nba", "bronze/raw_regular_season.csv", "include/datasets/Regular_Season.csv")
        print(f"File 'nba_regular_season.csv' uploaded successfully")

    @task()
    def bronze_to_silver_nba_dataset():
        from io import BytesIO

        client = client = utils.get_minio_client()

        raw_nba = client.get_object("nba", "bronze/nba_raw.csv")

        df_nba = pd.read_csv(raw_nba)

        columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP", "MIN", "FGM"]
        df_nba = df_nba[columns]

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

        df_nba = df_nba.rename(columns=renamed_columns)

        df_nba["player_id"] = df_nba["player_id"].astype(int)
        df_nba["rank"] = df_nba["rank"].astype(int)
        df_nba["games_played"] = df_nba["games_played"].astype(int)
        df_nba["minutes"] = df_nba["minutes"].astype(int)
        df_nba["field_goals_made"] = df_nba["field_goals_made"].astype(int)

        data = BytesIO()
        df_nba.to_parquet(data, index=False)
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
        from io import BytesIO

        client = client = utils.get_minio_client()

        raw_playoffs = client.get_object("nba", "bronze/raw_playoffs.csv")

        df_playoffs = pd.read_csv(raw_playoffs)
        columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP"]
        df_playoffs = df_playoffs[columns]

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

        df_playoffs = df_playoffs.rename(columns=renamed_columns)


        df_playoffs["player_id"] = df_playoffs["player_id"].astype(int)
        df_playoffs["rank"] = df_playoffs["rank"].astype(int)
        df_playoffs["games_played"] = df_playoffs["games_played"].astype(int)


        data = BytesIO()
        df_playoffs.to_parquet(data, index=False)
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
        from io import BytesIO

        client = client = utils.get_minio_client()

        raw_regular_season = client.get_object("nba", "bronze/raw_regular_season.csv")

        df_regular_season = pd.read_csv(raw_regular_season)
        columns = ["year", "Season_type", "PLAYER_ID", "RANK", "PLAYER", "TEAM_ID", "TEAM", "GP"]
        df_regular_season = df_regular_season[columns]

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

        df_regular_season = df_regular_season.rename(columns=renamed_columns)


        df_regular_season["player_id"] = df_regular_season["player_id"].astype(int)
        df_regular_season["rank"] = df_regular_season["rank"].astype(int)
        df_regular_season["games_played"] = df_regular_season["games_played"].astype(int)


        data = BytesIO()
        df_regular_season.to_parquet(data, index=False)
        length_bytes = data.tell()
        data.seek(0)

        client.put_object(
            bucket_name="nba",
            object_name="silver/regular_season.parquet",
            length=length_bytes,
            data=data
            )

        print(f"File 'regular_season.parquet' uploaded successfully")



    chain(
        create_nba_bucket(),
        [upload_nba_raw_dataset(),
        upload_playoff_dataset(),
        upload_regular_season_dataset()],
        bronze_to_silver_nba_dataset(),
        bronze_to_silver_playoff_dataset(),
        bronze_to_silver_regular_season(),
    )

nba()