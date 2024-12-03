def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query

def tables_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1