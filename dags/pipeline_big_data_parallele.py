import csv
import json
import os

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
CLEAN_FILE = f"{DATA_DIR}/ventes_clean_parallel.csv"
RESULT_VILLE = f"{DATA_DIR}/resultat_par_ville.json"
RESULT_PRODUIT = f"{DATA_DIR}/resultat_par_produit.json"
REPORT_FILE = f"{DATA_DIR}/rapport_parallel.txt"


def preparation_donnees():
    os.makedirs(DATA_DIR, exist_ok=True)
    ventes = [
        ["id_vente", "ville", "produit", "prix", "quantite", "montant"],
        [1, "Casablanca", "PC", 8000, 2, 16000],
        [2, "Rabat", "Clavier", 300, 5, 1500],
        [3, "Marrakech", "Souris", 150, 10, 1500],
        [4, "Casablanca", "Ecran", 2500, 3, 7500],
        [5, "Tanger", "PC", 8500, 1, 8500],
        [6, "Rabat", "Ecran", 2300, 2, 4600],
    ]
    with open(CLEAN_FILE, "w", newline="", encoding="utf-8") as file:
        csv.writer(file).writerows(ventes)
    print("Donnees preparees avec succes.")


def validation_donnees():
    if not os.path.exists(CLEAN_FILE):
        raise FileNotFoundError("Le fichier nettoye n'existe pas.")
    print("Validation reussie.")


def _analyser(cle, fichier_resultat):
    resultats = {}
    with open(CLEAN_FILE, encoding="utf-8") as file:
        for row in csv.DictReader(file):
            valeur = row[cle]
            resultats[valeur] = resultats.get(valeur, 0) + float(row["montant"])
    with open(fichier_resultat, "w", encoding="utf-8") as file:
        json.dump(resultats, file, indent=4, ensure_ascii=False)
    return resultats


def traitement_par_ville():
    print(f"Traitement par ville termine : {_analyser('ville', RESULT_VILLE)}")


def traitement_par_produit():
    print(f"Traitement par produit termine : {_analyser('produit', RESULT_PRODUIT)}")


def generation_rapport_final():
    with open(RESULT_VILLE, encoding="utf-8") as file:
        par_ville = json.load(file)
    with open(RESULT_PRODUIT, encoding="utf-8") as file:
        par_produit = json.load(file)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("Rapport final du pipeline parallele\n==================================\n\n")
        report.write("Chiffre d'affaires par ville\n")
        for ville, ca in par_ville.items():
            report.write(f"{ville} : {ca} DH\n")
        report.write("\nChiffre d'affaires par produit\n")
        for produit, ca in par_produit.items():
            report.write(f"{produit} : {ca} DH\n")
    print(f"Rapport final genere avec succes : {REPORT_FILE}")


with DAG(
    dag_id="pipeline_big_data_parallele",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["big-data", "parallelisme", "python-operator"],
) as dag:
    preparation = PythonOperator(task_id="preparation_donnees", python_callable=preparation_donnees)
    validation = PythonOperator(task_id="validation_donnees", python_callable=validation_donnees)
    analyse_ville = PythonOperator(task_id="traitement_par_ville", python_callable=traitement_par_ville)
    analyse_produit = PythonOperator(task_id="traitement_par_produit", python_callable=traitement_par_produit)
    rapport = PythonOperator(task_id="generation_rapport_final", python_callable=generation_rapport_final)

    preparation >> validation >> [analyse_ville, analyse_produit] >> rapport
