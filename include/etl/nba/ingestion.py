
from airflow.decorators import task
from include.utils import utils

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