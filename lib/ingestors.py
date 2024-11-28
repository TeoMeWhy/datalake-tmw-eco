import json

import delta
from pyspark.sql.types import StructField, StructType

class IngestorBronzeFullLoad:

    def __init__(self, spark, database, table, file_format):
        self.spark = spark
        self.database = database
        self.table = table
        self.file_format = file_format
        self.path = f"/Volumes/raw/dms/full-load/{self.database}/{self.table}"
        self.tablename = f"bronze.{self.database}.{self.table}"
    
    def load(self):
        df = (self.spark.read
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

    def __init__(self, spark, database, table, file_format, pk_field, updated_field):
        self.spark = spark
        self.database = database
        self.table = table
        self.file_format = file_format
        self.pk_field = pk_field
        self.updated_field = updated_field
        self.path = f"/Volumes/raw/dms/cdc/{self.database}/{self.table}"
        self.tablename = f"bronze.{self.database}.{self.table}"

    def load(self):
        df = (self.spark.read
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

        return self.spark.sql(query)

    def save(self, df):
        bronze_data = delta.DeltaTable.forName(self.spark, self.tablename)

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


class IngestorBronzeCDCStreaming:

    def __init__(self, spark, database, table, file_format, pk_field, updated_field):
        self.spark = spark
        self.database = database
        self.table = table
        self.file_format = file_format
        self.pk_field = pk_field
        self.updated_field = updated_field
        self.path = f"/Volumes/raw/dms/cdc/{self.database}/{self.table}"
        self.checkpoint_path = f"/Volumes/raw/dms/cdc/{self.database}/checkpoint_{self.table}"
        self.tablename = f"bronze.{self.database}.{self.table}"
        self.schema = self.load_schema()

    def load_schema(self):
        path = f"{self.database}/{self.table}/schema.json"
        with open(path, "r") as open_file:
            schema = json.load(open_file)
        return StructType.fromJson(schema) 

    def load(self):
        df = (self.spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", self.file_format)
                  .schema(self.schema)
                  .load(self.path))
        return df

    def upsert(self, df, bronze_df):
        df.createOrReplaceGlobalTempView(f"{self.table}_bronze_cdc")
        query = f"""
            SELECT *
            FROM global_temp.{self.table}_bronze_cdc
            QUALIFY ROW_NUMBER() OVER (PARTITION BY {self.pk_field} ORDER BY {self.updated_field} DESC)=1
        """
        df_unique = self.spark.sql(query)

        (bronze_df.alias("old")
                  .merge(df_unique.alias("new"), f"old.{self.pk_field}=new.{self.pk_field}")
                  .whenMatchedDelete(condition=f"new.Op='D' AND new.{self.updated_field}>old.{self.updated_field}")
                  .whenMatchedUpdateAll(condition=f"new.Op='U' AND new.{self.updated_field}>old.{self.updated_field}")
                  .whenNotMatchedInsertAll()
                  .execute())

    def save(self, df):
        bronze_df = delta.DeltaTable.forName(self.spark, self.tablename)
        stream = (df.writeStream
                    .option("checkpointLocation", self.checkpoint_path)
                    .foreachBatch(lambda df, batchId: self.upsert(df, bronze_df))
                    .trigger(availableNow=True)) # para o stream quando nao tem mais dados
        return stream

    def run(self):
        df_stream = self.load()
        stream = self.save(df_stream)
        return stream.start()