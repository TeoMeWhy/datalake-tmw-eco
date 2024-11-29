# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

from ingestors import IngestorBronzeFullLoad, IngestorBronzeCDCStreaming
import utils

# COMMAND ----------

path = "/Volumes/raw/dms/full-load/points/products/"

df = spark.read.format("parquet").load(path)
df.schema.json()

# COMMAND ----------

catalog = 'bronze'
database = dbutils.widgets.get('database')
table = dbutils.widgets.get('table')
file_format = 'parquet'
pk_field = dbutils.widgets.get('pk_field')
updated_field = dbutils.widgets.get('updated_field')

ingest_fullload = IngestorBronzeFullLoad(spark=spark,
                                         database=database,
                                         table=table,
                                         file_format=file_format)

ingest_cdc_streaming = IngestorBronzeCDCStreaming(spark=spark,
                                                  database=database,
                                                  table=table,
                                                  file_format=file_format,
                                                  pk_field=pk_field,
                                                  updated_field=updated_field)

# COMMAND ----------

if not utils.tables_exists(spark, catalog, database, table):
    print("Criando tabela com carga full-load...")
    ingest_fullload.run()
    dbutils.fs.rm(ingest_cdc_streaming.checkpoint_path, True)
    print("ok")

# COMMAND ----------

print("Executando CDC...")
ingest_cdc_streaming.run()
print("ok")
