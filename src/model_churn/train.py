# Databricks notebook source
# MAGIC %pip install databricks-feature-engineering mlflow feature-engine

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
from sklearn import model_selection
from sklearn import tree
from sklearn import ensemble
from sklearn import pipeline
from sklearn import metrics
import mlflow

from feature_engine import imputation

import pandas as pd
pd.set_option('display.max_rows', 500)


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
training_set = fe.create_training_set(df=df,
                                      feature_lookups=lookups,
                                      label="flChurn")

df_train = training_set.load_df()

# COMMAND ----------

df_train_pandas = df_train.toPandas()
df_train_pandas.head()

# COMMAND ----------

# DBTITLE 1,SAMPLE
target = 'flChurn'
features = df_train_pandas.columns.tolist()[2:-1]
X = df_train_pandas[features]
y = df_train_pandas[target]

X_train, X_test, y_train, y_test = model_selection.train_test_split(X, y,
                                                                    test_size=0.3,
                                                                    random_state=42,
                                                                    stratify=y)

print("Taxa de resposta treino:", y_train.mean())
print("Taxa de resposta teste:", y_test.mean())

print("Tamanho da base de treino:", y_train.shape[0])
print("Tamanho da base de teste:", y_test.shape[0])

# COMMAND ----------

# DBTITLE 1,EXPLORE
X_train.isna().sum().sort_values(ascending=False).head(5)

# COMMAND ----------

# DBTITLE 1,EXPLORE
df_train_explore = X_train.copy()
df_train_explore[target] = y_train.copy()

describe = df_train_explore.groupby(target)[features].mean().T
describe['ratio'] = describe[1] / describe[0]
describe.head()

# COMMAND ----------

# DBTITLE 1,MODIFY
features_imput_zeros = [
    'nrPctPontosDailyLoot','nrQtdePontosVendaItemRPG',
    'nrPctPontosVendaItemRPG','nrQtdePontosDailyLoot',
    'nrQtdePontosChurn2pp','nrPctPontosChurn2pp',
    'nrQtdePontosAirflowLover','nrPctPontosRLover',
    'nrQtdePontosRLover','nrPctPontosAirflowLover',
    'nrQtdePontosChurn10pp','nrPctPontosChurn10pp',
    'nrQtdePontosChurn5pp','nrPctPontosChurn5pp',
    'nrPctPontosTrocaStreamElements','nrQtdePontosTrocaStreamElements',
    'nrQtdePontosNegDia','nrQtdePontosNegTransacao',
    'nrQtdePontosNegD7','nrQtdePontosNegD28',
    'nrQtdePontosNegD56','nrQtdePontosNegVida',
    'nrQtdePontosPresencaStreak','nrPctPontosPresencaStreak',
    'nrQtdePontosResgatarPonei','nrPctPontosResgatarPonei',
    'nrQtdePontosD7','nrQtdePontosPosD7',
    'nrQtdeTransacaoDiaD7','nrQtdeMensagemMinuto',
    'nrQtdePontosMinuto','nrQtdeTransacoesMinuto',
    'nrPctPontosChatMessage','nrQtdePontosChatMessage',
    'nrPctPontosListaPresenca','nrQtdePontosListaPresenca',
]

imputer_zeros = imputation.ArbitraryNumberImputer(variables=features_imput_zeros,
                                                  arbitrary_number=0)

# COMMAND ----------

# DBTITLE 1,MODEL
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(experiment_id=1867194499571631)

with mlflow.start_run() as run:

    mlflow.sklearn.autolog()

    clf = ensemble.AdaBoostClassifier(
        random_state=42,

    )

    param_grid = {
        "n_estimators": [50, 60, 80, 90, 100, 110],
        "learning_rate": [0.2, 0.15, 0.11, 0.1, 0.09, 0.08],
    }

    grid = model_selection.GridSearchCV(clf,
                                        param_grid,
                                        cv=5,
                                        n_jobs=1,
                                        verbose=4,
                                        refit=True,
                                        scoring="roc_auc")

    meu_pipeline = pipeline.Pipeline([('Imputacao de Zeros', imputer_zeros),
                                      ('Ajuste Algoritmo', grid)])

    meu_pipeline.fit(X_train, y_train)

    y_pred_train = meu_pipeline.predict(X_train)
    y_pred_test = meu_pipeline.predict(X_test)

    acc_train = metrics.accuracy_score(y_train, y_pred_train)
    acc_test = metrics.accuracy_score(y_test, y_pred_test)

    y_prob_train = meu_pipeline.predict_proba(X_train)[:,1]
    y_prob_test = meu_pipeline.predict_proba(X_test)[:,1]

    auc_train = metrics.roc_auc_score(y_train , y_prob_train)
    auc_test = metrics.roc_auc_score(y_test , y_prob_test)

    model_metrics = {
        "acc_train" : acc_train,
        "acc_test" : acc_test,
        "auc_train": auc_train,
        "auc_test": auc_test,
    }

    mlflow.log_metrics(model_metrics)

    fe.log_model(
        model=meu_pipeline,
        artifact_path="model_packaged",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name="feature_store.upsell.churn",
    )


# COMMAND ----------

# DBTITLE 1,ASSESS
pd.Series(model_metrics)

# COMMAND ----------

features = meu_pipeline[:-1].transform(X_train.head(1)).columns.tolist()

feature_imporance = pd.Series(grid.best_estimator_.feature_importances_, index=features)
feature_imporance = feature_imporance.sort_values(ascending=False)
feature_imporance

