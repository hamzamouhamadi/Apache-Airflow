import csv
import json
import os

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_DIR = "/opt/airflow/data"
RECEPTION_FILE = f"{DATA_DIR}/etudiants_recus.csv"
RAW_FILE = f"{DATA_DIR}/etudiants_raw.csv"
CLEAN_FILE = f"{DATA_DIR}/etudiants_clean.csv"
GROUPS_FILE = f"{DATA_DIR}/affectation_groupes.json"
STATS_FILE = f"{DATA_DIR}/statistiques_inscriptions.json"
REPORT_FILE = f"{DATA_DIR}/rapport_inscriptions.txt"


def reception_fichier():
    os.makedirs(DATA_DIR, exist_ok=True)
    etudiants = [
        ["id", "nom", "filiere", "niveau"],
        [1, "Amina El Amrani", "BDCC", "M2"],
        [2, "Youssef Alaoui", "BDCC", "M2"],
        [3, "Sara Bennani", "IA", "M1"],
        [4, "Omar Idrissi", "BDCC", "M2"],
        [5, "Imane Tazi", "IA", "M1"],
        [6, "Mehdi Naciri", "Data", "M1"],
    ]
    with open(RECEPTION_FILE, "w", newline="", encoding="utf-8") as file:
        csv.writer(file).writerows(etudiants)
    print(f"Reception du fichier des etudiants : {RECEPTION_FILE}")


def stockage_zone_brute():
    if not os.path.exists(RECEPTION_FILE):
        raise FileNotFoundError("Le fichier recu est introuvable.")
    with open(RECEPTION_FILE, "rb") as source, open(RAW_FILE, "wb") as destination:
        destination.write(source.read())
    print(f"Stockage du fichier dans la zone brute : {RAW_FILE}")


def validation_fichier():
    with open(RAW_FILE, encoding="utf-8") as file:
        reader = csv.reader(file)
        header = next(reader)
        lignes = list(reader)
    if header != ["id", "nom", "filiere", "niveau"]:
        raise ValueError(f"Schema invalide : {header}")
    if not lignes:
        raise ValueError("Le fichier ne contient aucun etudiant.")
    print(f"Validation du fichier reussie : {len(lignes)} inscriptions")


def nettoyage_donnees():
    lignes = []
    with open(RAW_FILE, encoding="utf-8") as file:
        for row in csv.DictReader(file):
            lignes.append({key: value.strip() for key, value in row.items()})
    with open(CLEAN_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=["id", "nom", "filiere", "niveau"])
        writer.writeheader()
        writer.writerows(lignes)
    print(f"Nettoyage des donnees termine : {CLEAN_FILE}")


def affectation_groupes():
    affectations = {}
    with open(CLEAN_FILE, encoding="utf-8") as file:
        for index, row in enumerate(csv.DictReader(file)):
            affectations[row["id"]] = {
                "nom": row["nom"],
                "groupe": f"G{index % 2 + 1}",
            }
    with open(GROUPS_FILE, "w", encoding="utf-8") as file:
        json.dump(affectations, file, indent=4, ensure_ascii=False)
    print(f"Affectation des etudiants aux groupes terminee : {GROUPS_FILE}")


def generation_statistiques():
    statistiques = {"total": 0, "par_filiere": {}, "par_niveau": {}}
    with open(CLEAN_FILE, encoding="utf-8") as file:
        for row in csv.DictReader(file):
            statistiques["total"] += 1
            for champ, rubrique in (("filiere", "par_filiere"), ("niveau", "par_niveau")):
                valeur = row[champ]
                statistiques[rubrique][valeur] = statistiques[rubrique].get(valeur, 0) + 1
    with open(STATS_FILE, "w", encoding="utf-8") as file:
        json.dump(statistiques, file, indent=4, ensure_ascii=False)
    print(f"Generation des statistiques terminee : {statistiques}")


def rapport_final():
    with open(GROUPS_FILE, encoding="utf-8") as file:
        groupes = json.load(file)
    with open(STATS_FILE, encoding="utf-8") as file:
        statistiques = json.load(file)
    with open(REPORT_FILE, "w", encoding="utf-8") as report:
        report.write("Rapport des inscriptions etudiantes\n===================================\n\n")
        report.write(f"Nombre total : {statistiques['total']}\n")
        report.write(f"Par filiere : {statistiques['par_filiere']}\n")
        report.write(f"Par niveau : {statistiques['par_niveau']}\n\n")
        report.write("Affectations aux groupes\n")
        for etudiant in groupes.values():
            report.write(f"- {etudiant['nom']} : {etudiant['groupe']}\n")
    print(f"Generation du rapport final terminee : {REPORT_FILE}")


with DAG(
    dag_id="pipeline_inscription_etudiants",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["mini-projet", "python-operator"],
) as dag:
    reception = PythonOperator(task_id="reception_fichier", python_callable=reception_fichier)
    stockage = PythonOperator(task_id="stockage_zone_brute", python_callable=stockage_zone_brute)
    validation = PythonOperator(task_id="validation_fichier", python_callable=validation_fichier)
    nettoyage = PythonOperator(task_id="nettoyage_donnees", python_callable=nettoyage_donnees)
    affectation = PythonOperator(task_id="affectation_groupes", python_callable=affectation_groupes)
    statistiques = PythonOperator(task_id="generation_statistiques", python_callable=generation_statistiques)
    rapport = PythonOperator(task_id="rapport_final", python_callable=rapport_final)

    reception >> stockage >> validation >> nettoyage
    nettoyage >> [affectation, statistiques] >> rapport
