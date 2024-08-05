
from airflow.decorators import dag,task_group
from pendulum import datetime
from airflow.models.baseoperator import chain
from include.etl.nba import ingestion
from include.etl.nba import to_silver as ts
from include.etl.nba import to_gold as tg

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"owner": "andreb"},
    tags=["nbadag"],
)
def nba():
    create_nba_bucket = ingestion.create_nba_bucket()

    upload_nba_bronze_dataset = ingestion.upload_nba_raw_dataset()

    nba_bronze_to_silver = ts.bronze_to_silver_nba_dataset()

    @task_group(group_id="to_gold_group")
    def to_gold_group():
        dim_player = tg.dim_player()
        dim_team = tg.dim_team()
        fact_nba = tg.fact_nba()


    chain(create_nba_bucket, upload_nba_bronze_dataset, nba_bronze_to_silver, to_gold_group())


nba()