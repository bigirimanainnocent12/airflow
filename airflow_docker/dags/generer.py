from datetime import datetime, timedelta
from airflow import DAG
import random
import pandas as pd
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
# 3. INSERTION DANS POSTGRESQL VIA AIRFLOW CONNECTION
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
# DÉFINITION DU DAG
# -----------------------------
with DAG(
    dag_id="genererpersonnes",
    default_args=default_args,
    schedule="@daily",
    description="DAG pour générer, transformer et stocker des personnes dans PostgreSQL via Airflow",
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

    end = EmptyOperator(task_id='fin')

    start >> generer_personn >> transformer >> sauvegarder >> end