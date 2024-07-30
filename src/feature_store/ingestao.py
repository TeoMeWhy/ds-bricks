# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient
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

# COMMAND ----------

    
query = import_query("fs_geral.sql")

dates = ['2024-02-01',
         '2024-03-01',
         '2024-04-01',
         '2024-05-01',
         '2024-06-01',
         '2024-07-01']

if not table_exists("feature_store", "upsell", "fs_geral"):

    print("Criando tabela...")

    df = spark.sql(query.format(dt_ref=dates.pop(0)))
    fe.create_table(
        name="feature_store.upsell.fs_geral",
        primary_keys=['dtRef', 'idCliente'],
        df=df,
        partition_columns=['dtRef'],
        schema=df.schema,
    )

for d in tqdm(dates):
    df = spark.sql(query.format(dt_ref=d))
    fe.write_table(name="feature_store.upsell.fs_geral",
                   df=df,
                   mode="merge")
