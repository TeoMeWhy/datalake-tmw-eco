# Databricks notebook source
from pyspark.sql import functions as F

df_customer = (spark.read
           .format("parquet")
           .load("/Volumes/raw/dms/cdc/points/customers")
           .withColumn("flEmail", F.when(F.col("desc_email").isNull(), 0).otherwise(1))
           .select("Op", "uuid", "nr_points", "flEmail", "updated_at")
           .filter("updated_at > now() - interval 2 day"))

(df_customer.coalesce(1)
   .write
   .format("parquet")
   .mode("overwrite")
   .save("/Volumes/raw/dms/consolidado/points/customers"))

# COMMAND ----------

from pyspark.sql import functions as F

df_transaction = (spark.read
           .format("parquet")
           .load("/Volumes/raw/dms/cdc/points/transactions/")
           .filter("created_at > now() - interval 2 day"))


(df_transaction.coalesce(1)
   .write
   .format("parquet")
   .mode("overwrite")
   .save("/Volumes/raw/dms/consolidado/points/transactions"))

# COMMAND ----------

from pyspark.sql import functions as F


df_transaction_ = (df_transaction.withColumn("id_transaction_", F.col("uuid"))
                                .select("id_transaction_"))

df_transaction_product = (spark.read
                               .format("parquet")
                               .load("/Volumes/raw/dms/cdc/points/transaction_products/"))

df_transaction_product = (df_transaction_product.join(
    df_transaction_,
    on=df_transaction_product.id_transaction == df_transaction_.id_transaction_,
    how="right").select("Op",
                        "uuid",
                        "id_transaction",
                        "cod_product",
                        "qtde_product",
                        "vl_product")
)

(df_transaction_product.coalesce(1)
                       .write
                       .format("parquet")
                       .mode("overwrite")
                       .save("/Volumes/raw/dms/consolidado/points/transaction_products"))

# COMMAND ----------

print("df_transaction:", df_transaction.count())
print("df_transaction_product:", df_transaction_product.count())

