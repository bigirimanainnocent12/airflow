from datetime import datetime, timedelta
from airflow import DAG
import random
import pandas as pd

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email  


# -----------------------------
# PARAMÈTRES DU DAG
# -----------------------------
default_args = {
    "owner": "bigirimana",
    "email_on_failure": False,
    "email_on_retry": False,
    "start_date": datetime(2025, 1, 15),
    "retries": 1,
    "depends_on_past": False,
    "retry_delay": timedelta(minutes=5)
}


# -----------------------------
# 1. GÉNÉRATION DES DONNÉES
# -----------------------------
def generer_personnes():
    rows = 50
    data = {
        "NOM": [f"personne_{i}" for i in range(1, rows + 1)],
        "Age": [round(random.uniform(15, 120), 1) for _ in range(rows)],
        "poids": [round(random.uniform(50, 120), 1) for _ in range(rows)],
        "SEXE": [random.choice(["MASCULIN", "FEMININ"]) for _ in range(rows)],
        "ville": [random.choice(["Paris", "Toulouse", "Lille", "Marseille", "Nice"]) for _ in range(rows)]
    }

    df = pd.DataFrame(data)
    return df.to_json(orient="records")


# -----------------------------
# 2. TRANSFORMATION DES DONNÉES
# -----------------------------
def transformer_data(**context):
    data = context["ti"].xcom_pull(task_ids="generer_personnes")
    df = pd.read_json(data)

    def age(age):
        if age < 18:
            return 'Enfant'
        elif age < 35:
            return "Jeunes adultes"
        elif age < 60:
            return "Adultes"
        else:
            return "Senior"

    df["Cat_age"] = df["Age"].apply(age)
    return df.to_json(orient="records")


# -----------------------------
# 3. INSERTION DANS POSTGRESQL
# -----------------------------
def sauvegarder_data(**context):
    data = context["ti"].xcom_pull(task_ids="transformer")
    df = pd.read_json(data)

    hook = PostgresHook(postgres_conn_id="postgres_donne_generer")
    conn = hook.get_conn()
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO personnes (nom, age, poids, sexe, ville, cat_age)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            row["NOM"],
            row["Age"],
            row["poids"],
            row["SEXE"],
            row["ville"],
            row["Cat_age"]
        ))

    conn.commit()
    cur.close()
    conn.close()


# -----------------------------
# 4. CALCUL DES STATISTIQUES
# -----------------------------
def calculer_stats(**context):
    data = context["ti"].xcom_pull(task_ids="transformer")
    df = pd.read_json(data)

    stats = {
        "age_moyen": round(df["Age"].mean(), 2),
        "poids_moyen": round(df["poids"].mean(), 2),
        "repartition_sexe": df["SEXE"].value_counts().to_dict(),
        "repartition_ville": df["ville"].value_counts().to_dict(),
        "repartition_cat_age": df["Cat_age"].value_counts().to_dict(),
    }

    return stats


# -----------------------------
# 5. ENVOI D’EMAIL (compatible Airflow 3.x)
# -----------------------------
def envoyer_email(**context):
    stats = context["ti"].xcom_pull(task_ids="calculer_stats")

    html = f"""
    <h2>Statistiques du DAG : generer_personnes_avec_ai</h2>
    <p>Voici les statistiques descriptives des données générées :</p>

    <ul>
        <li><b>Âge moyen :</b> {stats['age_moyen']}</li>
        <li><b>Poids moyen :</b> {stats['poids_moyen']}</li>
        <li><b>Répartition par sexe :</b> {stats['repartition_sexe']}</li>
        <li><b>Répartition par ville :</b> {stats['repartition_ville']}</li>
        <li><b>Répartition par catégorie d'âge :</b> {stats['repartition_cat_age']}</li>
    </ul>

    <p>Pipeline exécuté avec succès.</p>
    """

    send_email(
        to=["gatoziinnocent@gmail.com", "bigirimanainnocent596@gmail.com"],
        subject="Statistiques du DAG Airflow",
        html_content=html
    )


# -----------------------------
# DÉFINITION DU DAG
# -----------------------------
with DAG(
    dag_id="envoyermail",
    default_args=default_args,
    schedule="* * * * *",
    description="DAG complet avec statistiques et email (Airflow 3.1.5)",
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='debut')

    generer_personn = PythonOperator(
        task_id="generer_personnes",
        python_callable=generer_personnes
    )

    transformer = PythonOperator(
        task_id="transformer",
        python_callable=transformer_data
    )

    sauvegarder = PythonOperator(
        task_id="sauvegarder",
        python_callable=sauvegarder_data
    )

    stats = PythonOperator(
        task_id="calculer_stats",
        python_callable=calculer_stats
    )

    envoyer_mail = PythonOperator(
        task_id="envoyer_mail",
        python_callable=envoyer_email
    )

    end = EmptyOperator(task_id='fin')

    start >> generer_personn >> transformer >> sauvegarder >> stats >> envoyer_mail >> end
