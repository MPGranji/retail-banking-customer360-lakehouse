import oracledb


def connect(cfg: dict):
    ora = cfg["oracle"]
    dsn = f"{ora['host']}:{ora['port']}/{ora['service']}"
    return oracledb.connect(user=ora["user"], password=ora["password"], dsn=dsn)
