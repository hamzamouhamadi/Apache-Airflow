import csv
import json
import os
from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
RAW_FILE = f"{DATA_DIR}/ventes_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/ventes_clean.csv"
RESULT_FILE = f"{DATA_DIR}/resultats_ventes.json"
REPORT_FILE = f"{DATA_DIR}/rapport_pipeline.txt"


def ingestion_donnees():
    os.makedirs(DATA_DIR, exist_ok=True)
    ventes = [
        ["id_vente", "ville", "produit", "prix", "quantite"],
        [1, "Casablanca", "PC", 8000, 2],
        [2, "Rabat", "Clavier", 300, 5],
        [3, "Marrakech", "Souris", 150, 10],
        [4, "Casablanca", "Ecran", 2500, 3],
        [5, "Tanger", "PC", 8500, 1],
        [6, "Rabat", "Ecran", 2300, 2],
    ]
    with open(RAW_FILE, "w", newline="", encoding="utf-8") as file:
        csv.writer(file).writerows(ventes)
    print(f"Ingestion terminee. Fichier cree : {RAW_FILE}")


def stockage_zone_brute():
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier brut n'existe pas.")
    print("Stockage dans la zone brute termine.")
    print(f"Fichier brut : {RAW_FILE} ({os.path.getsize(RAW_FILE)} octets)")


def validation_donnees():
    if not os.path.exists(RAW_FILE):
        raise FileNotFoundError("Le fichier de donnees est introuvable.")
    with open(RAW_FILE, encoding="utf-8") as file:
        header = next(csv.reader(file))
    colonnes = ["id_vente", "ville", "produit", "prix", "quantite"]
    if header != colonnes:
        raise ValueError(f"Schema incorrect : {header}")
    print(f"Validation terminee avec succes. Colonnes detectees : {header}")


def transformation_donnees():
    lignes = []
    with open(RAW_FILE, encoding="utf-8") as input_file:
        for row in csv.DictReader(input_file):
            prix = float(row["prix"])
            quantite = int(row["quantite"])
            lignes.append(
                {
                    "id_vente": row["id_vente"],
                    "ville": row["ville"],
                    "produit": row["produit"],
                    "prix": prix,
                    "quantite": quantite,
                    "montant": prix * quantite,
                }
            )
    with open(CLEAN_FILE, "w", newline="", encoding="utf-8") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=lignes[0].keys())
        writer.writeheader()
        writer.writerows(lignes)
    print(f"Transformation terminee. Fichier cree : {CLEAN_FILE}")


def traitement_analytique():
    resultats = {}
    with open(CLEAN_FILE, encoding="utf-8") as file:
        for row in csv.DictReader(file):
            ville = row["ville"]
            resultats[ville] = resultats.get(ville, 0) + float(row["montant"])
    with open(RESULT_FILE, "w", encoding="utf-8") as file:
        json.dump(resultats, file, indent=4, ensure_ascii=False)
    print(f"Traitement analytique termine : {resultats}")


def chargement_resultats():
    if not os.path.exists(RESULT_FILE):
        raise FileNotFoundError("Le fichier des resultats est introuvable.")
    print(f"Chargement des resultats termine : {RESULT_FILE}")


def generation_rapport():
    with open(RESULT_FILE, encoding="utf-8") as file:
        resultats = json.load(file)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("Rapport du pipeline Big Data\n============================\n\n")
        for ville, ca in resultats.items():
            report.write(f"{ville} : {ca} DH\n")
    print(f"Rapport final genere : {REPORT_FILE}")


with DAG(
    dag_id="pipeline_big_data_python",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["big-data", "python-operator", "pipeline"],
) as dag:
    ingestion = PythonOperator(task_id="ingestion_donnees", python_callable=ingestion_donnees)
    stockage = PythonOperator(task_id="stockage_zone_brute", python_callable=stockage_zone_brute)
    validation = PythonOperator(task_id="validation_donnees", python_callable=validation_donnees)
    transformation = PythonOperator(task_id="transformation_donnees", python_callable=transformation_donnees)
    traitement = PythonOperator(task_id="traitement_analytique", python_callable=traitement_analytique)
    chargement = PythonOperator(task_id="chargement_resultats", python_callable=chargement_resultats)
    rapport = PythonOperator(task_id="generation_rapport", python_callable=generation_rapport)

    ingestion >> stockage >> validation >> transformation >> traitement >> chargement >> rapport
