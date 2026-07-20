import psycopg2


def connect(cfg: dict):
    pg = cfg["postgres"]
    return psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["dbname"],
        user=pg["user"],
        password=pg["password"],
    )
