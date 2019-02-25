import airflow

from airflow import DAG
from airflow_training.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.operators.slack_operator import SlackAPIPostOperator

dag = DAG(
    dag_id='godatafest',
    schedule_interval='@daily',
    default_args={
        'owner': 'GoDataDriven',
        'start_date': airflow.utils.dates.days_ago(2)
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


def send_to_slack_func(**context):
    operator = SlackAPIPostOperator(
        task_id='postTopCommitter',
        text=str(context['task_instance']),
        token='xoxp-559854890739-559228586160-561116849751-2c717700dd7b7a197765ac21770c9c08',
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
