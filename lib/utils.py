import datetime
from databricks.feature_engineering import FeatureLookup


def import_query(path):
    with open(path, "r") as open_file:
        query = open_file.read()
    return query


def tables_exists(spark, catalog, database, table):
    count = (spark.sql(f"SHOW TABLES FROM {catalog}.{database}")
                  .filter(f"tableName = '{table}'")
                  .count())
    return count == 1


def range_date(start, stop, month=False):
    dates = []
    dt_start = datetime.datetime.strptime(start, "%Y-%m-%d")
    dt_stop = datetime.datetime.strptime(stop, "%Y-%m-%d")
    while dt_start <= dt_stop:
        dates.append(dt_start.strftime("%Y-%m-%d"))
        dt_start += datetime.timedelta(days=1)
    
    if month:
        return [i for i in dates if i.endswith('-01')]
    
    return dates


def create_feature_lookup(spark, table_name, lookup_key):
    sufix = table_name.split("_")[-1]
    rename = {i:i+f"_{sufix}" for i in spark.table(table_name).columns if i not in lookup_key}
    lookup = FeatureLookup(table_name=table_name,
                           lookup_key=lookup_key,
                           rename_outputs=rename)
    return lookup 