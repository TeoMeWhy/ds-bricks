# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering mlflow feature-engine

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

mlflow.set_registry_uri("databricks-uc")

model_uri = "models:/feature_store.upsell.churn/4"

model_pyfunc = mlflow.pyfunc.load_model(model_uri)
run_id = model_pyfunc.metadata.run_id

model = mlflow.sklearn.load_model(f"runs:/{run_id}/model")

# COMMAND ----------

lookups = [
    FeatureLookup(table_name="feature_store.upsell.fs_geral", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_pontos", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_transacoes", lookup_key=['dtRef', 'idCliente']),
    FeatureLookup(table_name="feature_store.upsell.fs_dia_horario", lookup_key=['dtRef', 'idCliente']),
]

query = """
    SELECT dtRef,
           idCliente
    FROM feature_store.upsell.fs_geral
    WHERE dtRef = (SELECT max(dtRef) FROM feature_store.upsell.fs_geral)
"""

df = spark.sql(query)

fe = FeatureEngineeringClient()
predict_set = fe.create_training_set(df=df,
                                      feature_lookups=lookups,
                                      label=None)

df_predict = predict_set.load_df().toPandas()

# COMMAND ----------

proba_churn = model.predict_proba(df_predict[model.feature_names_in_])[:,1]
df_predict['proba_churn'] = proba_churn
df_predict[['dtRef', 'idCliente', 'proba_churn']]
