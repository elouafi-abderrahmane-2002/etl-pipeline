# 🔄 ETL Multi-Sources — Airflow + PySpark + MongoDB / Redshift

Dans un contexte industriel comme un DCOE, les données arrivent de partout :
fichiers JSON exportés par des systèmes SCADA, CSV issus de capteurs, logs
d'applications, exports manuels. Ces sources ont des formats différents,
des schémas différents, des fréquences d'actualisation différentes.

Ce projet construit un pipeline ETL robuste et orchestré par Apache Airflow,
capable d'ingérer des données JSON/CSV hétérogènes, de les transformer avec
PySpark, et de les charger vers deux destinations au choix : MongoDB (NoSQL)
pour les données semi-structurées, ou Amazon Redshift (RDBMS) pour l'analytique
SQL structurée.

---

## Architecture du pipeline

```
  Sources hétérogènes
  ┌──────────────────────────────────────────┐
  │  JSON  │  CSV  │  Logs  │  API exports   │
  └──────────────────────────────────────────┘
          │
          │  Stockage intermédiaire
          ▼
  ┌──────────────────────────────────────────┐
  │           AWS S3 Data Lake               │
  │                                          │
  │  s3://dcoe-datalake/                     │
  │  ├── raw/YYYY-MM-DD/       ← données     │
  │  │   ├── source_a.json     brutes        │
  │  │   └── source_b.csv                   │
  │  ├── processed/            ← transformées│
  │  └── rejected/             ← erreurs     │
  └──────────────────────────────────────────┘
          │
          │  Orchestration : Apache Airflow DAG
          ▼
  ┌──────────────────────────────────────────┐
  │              DAG : dcoe_etl              │
  │                                          │
  │  getLastDate → checkNewData              │
  │        │                                 │
  │        ├── [new data]                    │
  │        │      │                          │
  │        │  parseJson → sparkProcess       │
  │        │      │                          │
  │        │      ├──────────────────────┐   │
  │        │      ▼                      ▼   │
  │        │   MongoDB               Redshift│
  │        │   (NoSQL)               (RDBMS) │
  │        │                                 │
  │        └── [no new data] → endRun        │
  └──────────────────────────────────────────┘
```

---

## DAG Airflow — orchestration complète

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta

default_args = {
    'owner':            'dcoe-team',
    'depends_on_past':  False,
    'start_date':       datetime(2025, 1, 1),
    'retries':          2,
    'retry_delay':      timedelta(minutes=5),
    'email_on_failure': True,
}

with DAG(
    'dcoe_etl_pipeline',
    default_args    = default_args,
    schedule_interval = '0 6 * * *',   # tous les jours à 6h00
    catchup         = False,
    max_active_runs = 1,               # pas de runs parallèles
) as dag:

    get_last_date = PythonOperator(
        task_id='getLastProcessedDate',
        python_callable=get_last_date_from_mongo
    )

    check_new_data = BranchPythonOperator(
        task_id='checkNewData',
        python_callable=check_new_data_available
        # retourne 'parseJsonFile' ou 'endRun'
    )

    parse_json = PythonOperator(
        task_id='parseJsonFile',
        python_callable=parse_and_clean_json
    )

    spark_process = PythonOperator(
        task_id='sparkProcess',
        python_callable=run_spark_transformation
    )

    load_destination = PythonOperator(
        task_id='loadToDestination',
        python_callable=load_to_mongo_or_redshift
        # configurable via Airflow Variable : 'destination' = 'mongodb' | 'redshift'
    )

    end_run = DummyOperator(task_id='endRun')

    # Dépendances
    get_last_date >> check_new_data
    check_new_data >> [parse_json, end_run]
    parse_json >> spark_process >> load_destination >> end_run
```

---

## Transformation PySpark — nettoyage et structuration

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, DoubleType, StringType, TimestampType

spark = SparkSession.builder \
    .appName("DCOE-ETL") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

def transform_raw_data(input_path: str, output_path: str, date: str):
    """
    Transformation PySpark des données brutes depuis S3.
    """
    # Lecture multi-format depuis S3
    df_json = spark.read.json(f"s3://dcoe-datalake/raw/{date}/*.json")
    df_csv  = spark.read.option("header", "true") \
                        .option("inferSchema", "true") \
                        .csv(f"s3://dcoe-datalake/raw/{date}/*.csv")

    # Normalisation des schémas pour jointure
    df_json = df_json.select(
        F.col("device_id").cast(StringType()),
        F.col("timestamp").cast(TimestampType()),
        F.col("value").cast(DoubleType()),
        F.col("metric").cast(StringType()),
        F.lit("json").alias("source_format")
    )
    df_csv = df_csv.select(
        F.col("id").alias("device_id").cast(StringType()),
        F.col("ts").alias("timestamp").cast(TimestampType()),
        F.col("val").alias("value").cast(DoubleType()),
        F.col("name").alias("metric").cast(StringType()),
        F.lit("csv").alias("source_format")
    )

    # Union des deux sources
    df_combined = df_json.union(df_csv)

    # Nettoyage
    df_clean = df_combined \
        .dropDuplicates(["device_id", "timestamp", "metric"]) \
        .filter(F.col("value").isNotNull()) \
        .filter(F.col("timestamp").isNotNull()) \
        .withColumn("processed_at", F.current_timestamp()) \
        .withColumn("date_partition", F.to_date("timestamp"))

    # Contrôle qualité : compter les anomalies
    n_total = df_combined.count()
    n_clean = df_clean.count()
    print(f"Qualité : {n_clean}/{n_total} lignes retenues "
          f"({100*n_clean/n_total:.1f}%)")

    # Écriture Parquet partitionné par date
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("date_partition") \
        .parquet(output_path)

    return df_clean
```

---

## Chargement dual : MongoDB ou Redshift

```python
def load_to_mongo(df_spark, db_name: str, collection: str):
    """Chargement vers MongoDB pour données semi-structurées."""
    from pymongo import MongoClient

    # Spark → Python dict (pour petits volumes post-agrégation)
    records = df_spark.toPandas().to_dict('records')

    client = MongoClient(os.environ['MONGO_URI'])
    db     = client[db_name]

    # Upsert pour idempotence
    from pymongo import UpdateOne
    operations = [
        UpdateOne(
            {'device_id': r['device_id'], 'timestamp': r['timestamp']},
            {'$set': r},
            upsert=True
        )
        for r in records
    ]
    result = db[collection].bulk_write(operations)
    print(f"MongoDB: {result.upserted_count} insérés, {result.modified_count} mis à jour")


def load_to_redshift(df_spark, table: str):
    """Chargement vers Redshift via JDBC pour analytique SQL."""
    df_spark.write \
        .format("jdbc") \
        .option("url",      os.environ['REDSHIFT_URL']) \
        .option("dbtable",  table) \
        .option("user",     os.environ['REDSHIFT_USER']) \
        .option("password", os.environ['REDSHIFT_PASS']) \
        .option("driver",   "com.amazon.redshift.jdbc42.Driver") \
        .mode("append") \
        .save()
```

---

## Ce que j'ai appris

L'idempotence est non-négociable dans un pipeline en production. Si Airflow
reexécute une tâche après un échec (retry), le pipeline ne doit pas créer
de doublons. C'est pourquoi chaque chargement utilise un `upsert` (MongoDB)
ou un `DELETE + INSERT` (Redshift) plutôt qu'un simple `INSERT`.

Sans cette précaution, un seul timeout réseau peut doubler les données dans
la table de destination — et c'est souvent découvert des semaines plus tard
dans un rapport analytique qui donne des totaux absurdes.

---

*Projet réalisé dans le cadre de ma formation ingénieur — ENSET Mohammedia*
*Par **Abderrahmane Elouafi** · [LinkedIn](https://www.linkedin.com/in/abderrahmane-elouafi-43226736b/) · [Portfolio](https://my-first-porfolio-six.vercel.app/)*
