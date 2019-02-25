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

wait_tasks = [
    BashOperator(task_id="wait_" + str(w), bash_command="sleep " + str(w), dag=dag)
    for w in [1, 5, 10]
]

the_end = DummyOperator (
    task_id="the_end", dag=dag
)

the_start >> wait_tasks >> the_end
