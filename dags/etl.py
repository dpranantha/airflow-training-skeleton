import airflow
from airflow import DAG
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator

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
    bucket="dpranantha",
    filename="land_price_uk_{{ ds }}_{}.json",
    dag=dag,
)

http_to_gcs = HttpToGcsOperator(
    task_id="http_currency_converter",
    endpoint="/convert-currency?date={{ ds }}&from=GBP&to=EUR",
    gcs_bucket="dpranantha",
    gcs_path="currency_{{ ds }}_{}.json",
    http_conn_id="currency_converter",
    dag=dag
)

pgsl_to_gcs
http_to_gcs

