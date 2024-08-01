# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

def import_query(path):
    with open(path) as open_file:
        return open_file.read()

lookups = [
    FeatureLookup(table_name="feature_store.upsell.fs_geral", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_pontos", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_transacoes", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_dia_horario", lookup_key=['dtRef', 'idCliente']),    
]

query = import_query("fl_churn.sql")
df = spark.sql(query)

fe = FeatureEngineeringClient()
training_set = fe.create_training_set(df=df, feature_lookups=lookups, label="flChurn")
df_train = training_set.load_df()

# COMMAND ----------

df_train_pandas = df_train.toPandas()
df_train_pandas.head()

# COMMAND ----------


