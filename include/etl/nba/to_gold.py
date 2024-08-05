
from airflow.decorators import task
from include.utils import utils
import polars as pl
from io import BytesIO

@task()
def dim_player():
    client = utils.get_minio_client()

    silver_nba = client.get_object("nba", "silver/nba.parquet")

    df = pl.read_parquet(silver_nba)

    df = df.select(["player_id", "player_name"])

    df = df.unique(subset=["player_id"])

    data = BytesIO()
    df.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)


    client.put_object(
        bucket_name="nba",
        object_name="gold/dim_player.parquet",
        length=length_bytes,
        data=data
    )


@task()
def dim_team():
    client = utils.get_minio_client()

    silver_nba = client.get_object("nba", "silver/nba.parquet")

    df = pl.read_parquet(silver_nba)

    df = df.select(["team_id", "team_abbreviation"])

    df = df.unique(subset=["team_id"])

    team_dict = {
        "BKN": "Brooklyn Nets",
        "BOS": "Boston Celtics",
        "NYK": "New York Knicks",
        "PHI": "Philadelphia 76ers",
        "TOR": "Toronto Raptors",
        "CHI": "Chicago Bulls",
        "CLE": "Cleveland Cavaliers",
        "DET": "Detroit Pistons",
        "IND": "Indiana Pacers",
        "MIL": "Milwaukee Bucks",
        "ATL": "Atlanta Hawks",
        "CHA": "Charlotte Hornets",
        "MIA": "Miami Heat",
        "ORL": "Orlando Magic",
        "WAS": "Washington Wizards",
        "DEN": "Denver Nuggets",
        "MIN": "Minnesota Timberwolves",
        "OKC": "Oklahoma City Thunder",
        "POR": "Portland Trail Blazers",
        "UTA": "Utah Jazz",
        "GSW": "Golden State Warriors",
        "LAC": "LA Clippers",
        "LAL": "Los Angeles Lakers",
        "PHX": "Phoenix Suns",
        "SAC": "Sacramento Kings",
        "DAL": "Dallas Mavericks",
        "HOU": "Houston Rockets",
        "MEM": "Memphis Grizzlies",
        "NOP": "New Orleans Pelicans",
        "SAS": "San Antonio Spurs",
        "NOH": "New Orleans Hornets"
    }

    mapper = pl.DataFrame({
        "keys": list(team_dict.keys()),
        "values": list(team_dict.values())
    })

    df = df.join(mapper, left_on="team_abbreviation", right_on="keys", how="left")

    data = BytesIO()
    df.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)

    client.put_object(
        bucket_name="nba",
        object_name="gold/dim_team.parquet",
        length=length_bytes,
        data=data
    )

@task()
def fact_nba():
    client = utils.get_minio_client()

    silver_nba = client.get_object("nba", "silver/nba.parquet")

    df = pl.read_parquet(silver_nba)

    df = df.select(["Year", "season_type", "player_id", "team_id", "rank", "games_played", "minutes", "field_goals_made"])

    data = BytesIO()
    df.write_parquet(data)
    length_bytes = data.tell()
    data.seek(0)

    client.put_object(
        bucket_name="nba",
        object_name="gold/fact_nba.parquet",
        length=length_bytes,
        data=data
    )