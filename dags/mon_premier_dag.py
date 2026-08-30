import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


def debut_pipeline():
    print("Debut du pipeline Big Data")


def traitement_pipeline():
    print("Traitement en cours")
    print("Simulation d'une etape de traitement de donnees")


def fin_pipeline():
    print("Fin du pipeline Big Data")


with DAG(
    dag_id="mon_premier_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["initiation", "python-operator"],
) as dag:
    debut = PythonOperator(task_id="debut", python_callable=debut_pipeline)
    traitement = PythonOperator(
        task_id="traitement", python_callable=traitement_pipeline
    )
    fin = PythonOperator(task_id="fin", python_callable=fin_pipeline)

    debut >> traitement >> fin
