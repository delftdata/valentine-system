import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists

TAKEN_DB_NAMES = ["postgres", "template0", "template1"]


class DatabaseDoesNotExist(Exception):
    pass


def list_postgres_dbs(postgres_url: str) -> list[str]:
    engine = create_engine(postgres_url)
    result = engine.execute('SELECT datname FROM pg_database;').fetchall()
    result = [res[0] for res in result if res[0] not in TAKEN_DB_NAMES]
    return result


def list_postgres_db_tables(postgres_url: str, db_name: str) -> list[str]:
    engine = create_engine(str(postgres_url) + '/' + db_name)
    if not database_exists(engine.url):
        raise DatabaseDoesNotExist
    return engine.table_names()


def get_columns_from_postgres_table(postgres_url: str, db_name: str, table_name: str) -> list[str]:
    for df in pd.read_sql_table(table_name, str(postgres_url) + '/' + db_name, chunksize=1):
        return df.columns.tolist()


def get_pandas_df_from_postgres_table(postgres_url: str, db_name: str, table_name: str) -> pd.DataFrame:
    return pd.read_sql_table(table_name, str(postgres_url) + '/' + db_name)


def get_column_sample_from_postgres_table(postgres_url: str, db_name: str, table_name: str, column_name: str, n: int):
    for df in pd.read_sql_table(table_name, str(postgres_url) + '/' + db_name, chunksize=500):
        sample = df[column_name].dropna().tolist()[:n]
        if len(sample) < n:
            sample = sample + [''] * (n - len(sample))
        return sample
