# Databricks notebook source
import mlflow
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

mlflow.set_registry_uri("databricks-uc")

date = dbutils.widgets.get("date")

model_name = "feature_store.upsell.churn"
model_version = 4

model_uri = f"models:/{model_name}/{model_version}"

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

query = f"""
    SELECT dtRef,
           idCliente
    FROM feature_store.upsell.fs_geral
    WHERE dtRef = '{date}'
"""

df = spark.sql(query)

fe = FeatureEngineeringClient()
predict_set = fe.create_training_set(df=df,
                                      feature_lookups=lookups,
                                      label=None)

df_predict = predict_set.load_df().toPandas()

# COMMAND ----------

probas = model.predict_proba(df_predict[model.feature_names_in_])

df_model = df_predict[['dtRef', 'idCliente']].copy()
df_model['descModelName'] = model_name
df_model['nrModelVersion'] = model_version
columns = ['dtRef','descModelName', 'nrModelVersion', 'idCliente']

df_model[model.classes_] = probas
df_model = df_model.set_index(columns).stack().reset_index()
df_model.columns = columns + ['descLabel', 'nrProbLabel']

# COMMAND ----------

sdf = spark.createDataFrame(df_model)

query_delete = f"""
DELETE
FROM feature_store.upsell.models
WHERE dtRef = '{date}'
AND descModelName = '{model_name}'
"""

spark.sql(query_delete)

(sdf.write
    .format('delta')
    .mode('append')
    .partitionBy(['descModelName','dtRef'])
    .saveAsTable('feature_store.upsell.models'))
