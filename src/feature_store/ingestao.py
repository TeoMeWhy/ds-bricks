# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
import datetime
from tqdm import tqdm

fe = FeatureEngineeringClient()

def import_query(path):
    with open(path) as open_file:
        return open_file.read()


def table_exists(catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"tableName='{table}'")
                  .count())
    return count > 0

def range_date(start, stop):
    dt_start = datetime.datetime.strptime(start, '%Y-%m-%d')
    dt_stop = datetime.datetime.strptime(stop, '%Y-%m-%d')
    dates = []
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime('%Y-%m-%d'))
        dt_start += datetime.timedelta(days=1)
    return dates

# COMMAND ----------

catalog = "feature_store"
database = "upsell"
table = dbutils.widgets.get("table")
start = dbutils.widgets.get("dt_start")
stop = dbutils.widgets.get("dt_stop")

dates = range_date(start, stop)

tablename = f"{catalog}.{database}.{table}"

query = import_query(f"{table}.sql")

if not table_exists(catalog, database, table):

    print("Criando tabela...")

    df = spark.sql(query.format(dt_ref=dates.pop(0)))
    fe.create_table(
        name=tablename,
        primary_keys=['dtRef', 'idCliente'],
        df=df,
        partition_columns=['dtRef'],
        schema=df.schema,
    )

for d in tqdm(dates):
    df = spark.sql(query.format(dt_ref=d))
    fe.write_table(
        name=tablename,
        df=df,
        mode="merge"
    )
