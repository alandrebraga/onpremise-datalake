
from airflow.decorators import dag,task_group
from pendulum import datetime
from airflow.models.baseoperator import chain
from include.etl.nba import ingestion
from include.etl.nba import to_silver as ts

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb", "retries": 3},
    tags=["nbadag"],
)
def nba():
    create_nba_bucket = ingestion.create_nba_bucket()

    @task_group(group_id="ingestion_group")
    def ingestion_group():
        upload_nba_bronze_dataset = ingestion.upload_nba_raw_dataset()
        upload_playoff_bronze_dataset = ingestion.upload_playoff_dataset()
        upload_regular_season_bronze_dataset = ingestion.upload_regular_season_dataset()

    @task_group(group_id="to_silver_group")
    def to_silver_group():
        nba_bronze_to_silver = ts.bronze_to_silver_nba_dataset()
        playoff_bronze_to_silver = ts.bronze_to_silver_playoff_dataset()
        regular_season_bronze_to_silver = ts.bronze_to_silver_regular_season()

    @task_group(group_id="to_gold_group")
    def to_gold_group():
        pass

    chain(create_nba_bucket, [ingestion_group()], [to_silver_group()])


nba()