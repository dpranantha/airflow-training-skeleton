import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id="hello_airflow",
    default_args={
        "owner": "godatadriven",
        "start_date": airflow.utils.dates.days_ago(3),
    },
)

the_start = BashOperator(
    task_id="print_exec_date", bash_command="echo {{ execution_date }}", dag=dag
)

wait_5 = BashOperator(
    task_id="wait_5", bash_command="sleep 5", dag=dag
)

wait_1 = BashOperator(
    task_id="wait_1", bash_command="sleep 1", dag=dag
)

wait_10 = BashOperator(
    task_id="wait_10", bash_command="sleep 10", dag=dag
)

the_end = DummyOperator (
    task_id="the_end", dag=dag
)

the_start >> [wait_5, wait_1, wait_10] >> the_end
