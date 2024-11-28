# Databricks notebook source
import delta

class IngestorBronzeFullLoad:

    def __init__(self, database, table, file_format):
        self.database = database
        self.table = table
        self.file_format = file_format
        self.path = f"/Volumes/raw/dms/full-load/{self.database}/{self.table}"
        self.tablename = f"bronze.{self.database}.{self.table}"
    
    def load(self):
        df = (spark.read
                   .format(self.file_format)
                   .load(self.path))
        return df
    
    def save(self, df):
        (df.write
           .format("delta")
           .mode("overwrite")
           .option("overwriteSchema", "true")
           .saveAsTable(self.tablename))
        
    def run(self):
        df = self.load()
        self.save(df)


class IngestorBronzeCDC:

    def __init__(self, database, table, file_format, pk_field, updated_field):
        self.database = database
        self.table = table
        self.file_format = file_format
        self.pk_field = pk_field
        self.updated_field = updated_field
        self.path = f"/Volumes/raw/dms/cdc/{self.database}/{self.table}"
        self.tablename = f"bronze.{self.database}.{self.table}"

    def load(self):
        df = (spark.read
                   .format(self.file_format)
                   .load(self.path))
        return df
    
    def transform(self, df):
        df.createOrReplaceTempView(f"cdc_{self.database}_{self.table}")
        
        query = f"""
        SELECT *
        FROM cdc_{self.database}_{self.table}
        QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.pk_field} ORDER BY {self.updated_field} DESC)=1
        """

        return spark.sql(query)

    def save(self, df):
        bronze_data = delta.DeltaTable.forName(spark, self.tablename)

        (bronze_data.alias("old")
            .merge(df.alias("new"), f"old.{self.pk_field} = new.{self.pk_field}")
            .whenMatchedDelete(condition = f"new.Op = 'D' AND new.{self.updated_field} > old.{self.updated_field}")
            .whenMatchedUpdateAll(condition = f"new.Op ='U' AND new.{self.updated_field} > old.{self.updated_field}")
            .whenNotMatchedInsertAll()
            .execute())
        
    def run(self):
        df = self.load()
        df_transformed = self.transform(df)
        self.save(df_transformed)


# COMMAND ----------

ingest_fullload = IngestorBronzeFullLoad("points", "transactions", "parquet")
ingest_fullload.run()

# COMMAND ----------

ingest_cdc = IngestorBronzeCDC("points", "customers", "parquet", "uuid", "updated_at")
ingest_cdc.run()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), max(created_at), min(created_at) FROM bronze.points.transactions

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     uuid,
# MAGIC     desc_customer_name,
# MAGIC     nr_points,
# MAGIC     id_twitch,
# MAGIC     id_you_tube,
# MAGIC     id_blue_sky,
# MAGIC     id_instagram,
# MAGIC     created_at,
# MAGIC     updated_at
# MAGIC
# MAGIC FROM bronze.points.customers
# MAGIC WHERE desc_customer_name = 'rinoscronauta'
# MAGIC
# MAGIC order by nr_points desc
