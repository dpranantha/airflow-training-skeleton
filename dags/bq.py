import airflow

from airflow import DAG
from airflow_training.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

dag = DAG(
    dag_id='newone',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(15)
    }
)

bq_fetch_data = BigQueryGetDataOperator(
    task_id='bq_fetch_data',
    sql="""SELECT author.name, COUNT(author.name) topcommitter 
        FROM [bigquery-public-data.github_repos.commits]
        WHERE DATE(committer.date) = '{{ ds }}' 
        AND repo_name LIKE '%airflow%'
        GROUP BY author.name
        ORDER BY topcommitter desc
        LIMIT 5""",
    dag=dag
)

from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


def send_to_slack_func(execution_date, **context):
    operator = SlackAPIPostOperator(
        task_id='postTopCommitter',
        text=execution_date.strftime("%a")+": "+str(context['task_instance'].xcom_pull(task_ids='bq_fetch_data')),
        token=Variable.get('slack_token'),
        channel='general'
    )
    return operator.execute(context=context)


send_to_slack = PythonOperator(
    task_id='send_to_slack',
    python_callable=send_to_slack_func,
    provide_context=True,
    dag=dag,
)

bq_fetch_data >> send_to_slack
