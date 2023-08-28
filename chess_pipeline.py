import dlt
from chess import source
import os

def load_table_counts(p: dlt.Pipeline, *table_names: str):
    """Returns row counts for `table_names` as dict"""

    # try sql, could be other destination though
    query = "\nUNION ALL\n".join([f"SELECT '{name}' as name, COUNT(1) as c FROM {name}" for name in table_names])
    with p.sql_client() as c:
        with c.execute_query(query) as cur:
            rows = list(cur.fetchall())
            return {r[0]: r[1] for r in rows}


def load_players_games_example(start_month: str, end_month: str) -> None:
    """Constructs a pipeline that will load chess games of specific players for a range of months."""
    os.environ['NORMALIZE__SCHEMA_UPDATE_MODE'] = "freeze-and-raise"

    # configure the pipeline: provide the destination and dataset name to which the data should go
    pipeline = dlt.pipeline(
        import_schema_path="schemas/import",
        export_schema_path="schemas/export",
        pipeline_name="chess_pipeline",
        destination='duckdb',
        dataset_name="chess_players_games_data",
        full_refresh=True
    )
    # create the data source by providing a list of players and start/end month in YYYY/MM format
    data = source(
        ["magnuscarlsen", "vincentkeymer", "dommarajugukesh", "rpragchess"],
        start_month=start_month,
        end_month=end_month,
    )
    # load the "players_games" and "players_profiles" out of all the possible resources
    info = pipeline.run(data.with_resources("players_games", "players_profiles"))
    print(info)
    table_counts = load_table_counts(pipeline, *[t["name"] for t in pipeline.default_schema.data_tables()])
    print(table_counts)


def load_players_games_incrementally() -> None:
    """Pipeline will not load the same game archive twice"""
    # loads games for 11.2022
    load_players_games_example("2022/11", "2022/11")
    # second load skips games for 11.2022 but will load for 12.2022
    load_players_games_example("2022/11", "2022/12")


if __name__ == "__main__":
    # run our main example
    load_players_games_example("2022/11", "2022/12")
