# Databricks notebook source
import sys
from databricks.feature_engineering import FeatureEngineeringClient

sys.path.insert(0,'../../../lib')

import utils

fe = FeatureEngineeringClient()

# COMMAND ----------

query = utils.import_query("target.sql")
df_target = spark.sql(query)

tables = ["feature_store.points.user_life",
          "feature_store.points.user_d7",
          "feature_store.points.user_d14",
          "feature_store.points.user_d28"]

fs_lookups = [utils.create_feature_lookup(spark, i, ["dt_ref", 'id_cliente']) for i in tables]

abt_spark = fe.create_training_set(df=df_target,
                                   feature_lookups=fs_lookups,
                                   label="flag_churn",
                                   exclude_columns=['qtde_ativacao'])

abt = abt_spark.load_df()

# COMMAND ----------

(abt.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("sandbox.teomewhy.abt_churn_20241217"))

# COMMAND ----------

abt.display()
