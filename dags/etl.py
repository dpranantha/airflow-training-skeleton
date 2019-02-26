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
from airflow_training.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
# from airflow.contrib.operators.dataflow_operator import DataFlowPythonOperator

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
    filename="{{ ds }}/land_price_uk_{}.json",
    dag=dag,
)

currencies = [
    HttpToGcsOperator(
        task_id="get_currency_" + currency,
        endpoint="convert-currency?date={{ ds }}&from=GBP&to=" + currency,
        gcs_bucket="dpranantha",
        gcs_path="{{ ds }}/currency_" + currency + ".json",
        http_conn_id="currency_converter",
        dag=dag,
    ) for currency in ['EUR', 'USD']
]

dataproc_create_cluster = DataprocClusterCreateOperator(
    task_id="create_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-4b5ba3f7fec9aea9",
    num_workers=2,
    zone="europe-west4-a",
    dag=dag,
)

compute_aggregates = DataProcPySparkOperator(
    task_id='compute_aggregates',
    main='gs://dpranantha/statistics/build_statistics.py',
    cluster_name='analyse-pricing-{{ ds }}',
    arguments=[
        "gs://dpranantha/{{ ds }}/land_price_uk_*.json",
        "gs://dpranantha/{{ ds }}/currency_*.json",
        "gs://dpranantha/{{ ds }}/average/"
    ],
    dag=dag,
)

dataproc_delete_cluster = DataprocClusterDeleteOperator(
    task_id="delete_dataproc",
    cluster_name="analyse-pricing-{{ ds }}",
    project_id="airflowbolcom-4b5ba3f7fec9aea9",
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

gcsBq = GoogleCloudStorageToBigQueryOperator(
    task_id="write_to_bq",
    bucket="dpranantha",
    source_objects=["{{ ds }}/average/*.parquet"],
    destination_project_dataset_table="airflowbolcom-4b5ba3f7fec9aea9:airflow.land_registry_price${{ ds_nodash }}",
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag, )

# load_into_bigquery = DataFlowPythonOperator(
#     task_id="land_registry_prices_to_bigquery",
#     dataflow_default_options={
#         'region': "europe-west1",
#         'input': 'gs://dpranantha/{{ ds }}/land_price_uk_*.json',
#         'table': 'land_registry_price_dataflow',
#         'dataset': 'airflow',
#         'project': 'airflowbolcom-4b5ba3f7fec9aea9',
#         'bucket': 'dpranantha',
#         'name': '{{ task_instance_key_str }}'
#     },
#     py_file="gs://statistics/dataflow_job.py",
#     dag=dag,
# )

pgsl_to_gcs >> dataproc_create_cluster
currencies >> dataproc_create_cluster
dataproc_create_cluster >> compute_aggregates >> dataproc_delete_cluster
dataproc_delete_cluster >> gcsBq
