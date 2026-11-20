'''
Load: isi tabel star/snowflake schema di PostgreSQL dari hasil OLAP CSV.
Tabel dibuat dulu menggunakan DDL schema di PostgreSQL (database: DB_project_3).
Supaya DAG bisa dijalankan berulang, tabel dikosongkan (TRUNCATE) sebelum diisi.
'''
import os

import psycopg2
from pyspark.sql import SparkSession, functions as F

INPUT = "/workspace/data/Olap"

TABLES = [
    'dim_country',
    'dim_us_state',
    'dim_date',
    'dim_data_center',
    'fact_grid_country',
    'fact_grid_state',
    'fact_infrastructure',
]

env = os.environ
DB_NAME = env.get('PG_DB', 'DB_project_3')
JDBC_URL = f"jdbc:postgresql://{env['PG_HOST']}:{env['PG_PORT']}/{DB_NAME}"
JDBC_PROPS = {
    'user': env['PG_USER'],
    'password': env['PG_PASSWORD'],
    'driver': 'org.postgresql.Driver',
}


def read_olap(spark):
    '''Baca hasil transform OLAP CSV. Kalau file belum ada, error di sini sebelum menyentuh DB.'''
    data = {}
    for name in TABLES:
        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .option("multiLine", True)
            .option("quote", '"')
            .option("escape", '"')
            .csv(f"{INPUT}/{name}.csv")
        )
        if name == "fact_infrastructure":
            df = (
                df.withColumn("record_date", F.to_date("record_date"))
                .withColumn(
                    "pue",
                    F.when((F.col("pue") >= 1) & (F.col("pue") <= 2), F.col("pue")).otherwise(None),
                )
            )
        data[name] = df
    return data


def truncate_tables():
    '''Kosongkan tabel dimensi & fakta (fact ikut terhapus lewat CASCADE).'''
    conn = psycopg2.connect(
        host=env['PG_HOST'],
        port=env['PG_PORT'],
        dbname=DB_NAME,
        user=env['PG_USER'],
        password=env['PG_PASSWORD'],
        connect_timeout=5,
    )
    with conn, conn.cursor() as cur:
        cur.execute(f"TRUNCATE {', '.join(TABLES)} CASCADE")
    conn.close()
    print('Tabel dikosongkan')


def load(olap_data):
    '''mode="append" supaya primary key, foreign key, dan check constraint dari DDL tetap terjaga.'''
    for name in TABLES:
        olap_data[name].write.jdbc(JDBC_URL, name, mode='append', properties=JDBC_PROPS)
        print('masuk:', name)


def check_result(spark):
    '''Verifikasi hasil load dengan query join antar tabel dimensi dan fakta di PostgreSQL.'''
    query = '''
    (SELECT d.country_name,
            COUNT(DISTINCT d.datacenter_name) AS total_dc,
            ROUND(SUM(f.total_power_mw)::numeric, 2)::float AS total_power_mw
     FROM fact_infrastructure f
     JOIN dim_data_center d ON f.datacenter_name = d.datacenter_name
     WHERE f.is_actual = TRUE
     GROUP BY d.country_name
     ORDER BY total_power_mw DESC
     LIMIT 10) AS t
    '''
    spark.read.jdbc(JDBC_URL, query, properties=JDBC_PROPS).show()


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dc-electricity-load").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    olap_data = read_olap(spark)
    truncate_tables()
    load(olap_data)
    check_result(spark)

    spark.stop()