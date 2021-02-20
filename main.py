from cassandra.cluster import Cluster
from pyspark.sql import SparkSession


def main():
    bootstrapCassandraConnection()
    spark: SparkSession = bootstrapSparkSession()

    uploadRawData(spark)
    aggregateByMonth(spark)


def aggregateByMonth(spark):
    spark \
        .read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="report_data_raw", keyspace="finance") \
        .load() \
        .createOrReplaceTempView('report_data_raw')
    with open('./queries/categories_by_months.sql') as sqlFile:
        spark \
            .sql(sqlFile.read()) \
            .write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="categories_by_months", keyspace="finance") \
            .mode("append") \
            .save()


def uploadRawData(spark):
    df = spark.read \
        .csv('file:///home/david/PycharmProjects/etl/storage/financeData.csv', header=True)
    csvData = df.rdd.map(mapToSchema).toDF(sampleRatio=0.01)
    csvData \
        .write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="report_data_raw", keyspace="finance") \
        .mode("append") \
        .save()


def bootstrapSparkSession():
    # "spark://0.0.0.0:7077"
    return SparkSession.builder \
        .master("local[3]") \
        .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.12:3.0.0") \
        .config('spark.sql.extensions', "com.datastax.spark.connector.CassandraSparkExtensions") \
        .config('spark.cassandra.connection.host', "0.0.0.0") \
        .config("spark.sql.catalog.etl", "com.datastax.spark.connector.datasource.CassandraCatalog") \
        .appName("Finance report") \
        .getOrCreate()


def bootstrapCassandraConnection():
    cluster = Cluster(['172.31.0.2', '172.31.0.5', '172.31.0.4', '172.31.0.3'])
    session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS finance "
                    "WITH REPLICATION = { "
                    "'class' : 'NetworkTopologyStrategy',"
                    "'ATL2': 1,"
                    "'SUW1': 1,"
                    "'DFW1': 1"
                    "};")
    session.execute("use finance")
    session.execute(open('./migrations/v1.cql').read())
    session.execute(open('./migrations/v2.cql').read())
    return session


def mapToSchema(x):
    return {
        "ref_date": x['REF_DATE'] + '-01',
        "category": x["North American Industry Classification System (NAICS)"],
        "statement_component": x["Balance sheet and income statement components"],
        "release": x["Release"],
        "uom": x["UOM"],
        "scalar_factor": x["SCALAR_FACTOR"],
        "vector": x["VECTOR"],
        "coordinate": x['COORDINATE'],
        "value": x['VALUE'],
        "status": x['STATUS'],
        "symbol": x['SYMBOL'],
        "terminated": x['TERMINATED'],
        "decimals": x['DECIMALS'],
    }


if __name__ == '__main__':
    main()
