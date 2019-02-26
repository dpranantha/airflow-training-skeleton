import airflow
from airflow import DAG
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator

dag = DAG(
    dag_id="etl_airflow",
    default_args={
        "owner": "etl_house",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)


pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
    task_id="pg_to_bq",
    postgres_conn_id="pg_landprice",
    sql="select * from land_registry_price_paid_uk where transfer_date = '{{ ds }}' ",
    bucket="gs://dpranantha",
    filename="land_price_uk.csv",
    dag=dag,
)

pgsl_to_gcs