def tables_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1