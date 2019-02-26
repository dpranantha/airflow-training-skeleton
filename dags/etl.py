import airflow
from airflow import DAG
from airflow_training.operators.postgres_to_gcs import PostgresToGoogleCloudStorageOperator
from airflow_training.operators.http_to_gcs import HttpToGcsOperator
from airflow.contrib.operators.dataproc_operator import (
    DataprocClusterCreateOperator,
    DataprocClusterDeleteOperator,
    DataProcPySparkOperator,
)
from airflow.utils.trigger_rule import TriggerRule

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

currencies = [
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        endpoint="convert-currency?date={{ ds }}&from=GBP&to=" + currency,
        gcs_bucket="dpranantha",
        gcs_path="currency_{{ ds }}_" + currency + "_{}.json",
        http_conn_id="currency_converter",
        dag=dag,
    ) for currency in ['EUR', 'USD']
]

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-4b5ba3f7fec9aea9",
    num_workers=1,
    num_preemptible_workers=1,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://dpranantha/statistics/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=[
        "gs://dpranantha/currency_{{ ds }}*.json",
        "gs://dpranantha/land_price_uk_{{ ds }}*.json"],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-4b5ba3f7fec9aea9",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

pgsl_to_gcs >> dataproc_create_cluster
currencies >> dataproc_create_cluster
dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
