# Databricks notebook source
import sys

sys.path.insert(0, "../lib")

from ingestors import IngestorBronzeFullLoad, IngestorBronzeCDCStreaming
import utils

# COMMAND ----------

catalog = 'bronze'
database = 'points'
table = 'customers'
file_format = 'parquet'
pk_field = 'uuid'
updated_field = 'updated_at'

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
