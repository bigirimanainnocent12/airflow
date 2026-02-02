from airflow import DAG
from airflow.sdk import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from pathlib import Path
from mesfonctions.fonctions import (
    extraction_donnees_meteo_current,
    extraction_donnees_meteo_previsions,
    transformation_donnees_meteo,
    transformation_donnees_meteo_previsions,
    sauvegarder_data,envoyer_mail_Début,envoyer_mail_fin,
    sauvegarder_data_previsions,
    create_table_meteo
)



default_args = {
    'owner': 'bigirimana',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
} 

# Définition du DAG
with DAG(
    dag_id='meteo',
    default_args=default_args,
    description='Pipeline de collecte et traitement des données météo de Toulouse',
    schedule="@daily",  
    catchup=False,
    tags=['weather', 'toulouse', 'open-meteo'],
) as dag:

    start=EmptyOperator(
        
        task_id='debut_pipeline'
               )
    statut_api=HttpSensor(
        task_id='statut_api_open_meteo',
        http_conn_id='open_meteo_api',
        endpoint=f'v1/forecast?latitude=0&longitude=0',
        response_check=lambda response: response.status_code == 200,
        poke_interval=5,
        timeout=20
                        )       

    with TaskGroup("Extraction_donne_meteo") as Extraction_donne_meteo:
            # Tâche 1: Extraction des données
        Debut_api=EmptyOperator( task_id='debut_extraction_donnees_meteo'
                                        )   
        extraction_task_current = PythonOperator(
        task_id='extraction_donnees_meteo',
        python_callable=extraction_donnees_meteo_current,
                                        )
        extraction_task_previsions = PythonOperator(
        task_id='extraction_donnees_meteo_previsions',
        python_callable=extraction_donnees_meteo_previsions,
                                        )
        final_extraction=EmptyOperator(
            task_id='fin_extraction_donnees_meteo'
                                        )                                
        Debut_api >> [extraction_task_current, extraction_task_previsions] >> final_extraction

    with TaskGroup("transformation_donnees") as transformation_donnees_saving:

        Debut_transformation = EmptyOperator(
        task_id='debut_transformation_donnees'
    )

        transform_task_current = PythonOperator(
        task_id='transform_weather_current',
        python_callable=transformation_donnees_meteo,
    )

        transform_task_previsions = PythonOperator(
        task_id='transform_weather_previsions',
        python_callable=transformation_donnees_meteo_previsions,
    )
        fin_transformation = EmptyOperator(
        task_id='fin_transformation_donnees'
                                          )
        Debut_transformation >> [transform_task_current, transform_task_previsions] >> fin_transformation 

    with TaskGroup("sauvegarde_donnees") as sauvegarde_donnees:
        Debut_sauvegarde = EmptyOperator(
        task_id='debut_sauvegarde_donnees'
          )
        create_table = PythonOperator(
        task_id="create_table_meteo",
        python_callable=create_table_meteo)

        save_base_current = PythonOperator(
        task_id="sauvegarder_data_current",
        python_callable=sauvegarder_data
    )

        save_base_previsions = PythonOperator(
        task_id="sauvegarder_data_previsions",
        python_callable=sauvegarder_data_previsions
    )
        fin_sauvegarde = EmptyOperator(
        task_id='fin_sauvegarde_donnees'
          )

        get_max_date = SQLExecuteQueryOperator(
                task_id="get_max_date",
                conn_id="api_mete_cloud",
                sql="SELECT MAX(date_horaire) FROM meteo_previsions;",
                 do_xcom_push=True # permet de récupérer la date dans une autre tâche
                                            )

        Debut_sauvegarde >> create_table >> [save_base_current, get_max_date]
        get_max_date >> save_base_previsions >> fin_sauvegarde
        save_base_current >> fin_sauvegarde

    final = EmptyOperator(
        task_id='fin_pipeline'
    )


    Envoyer_mail_Debut=PythonOperator(
        task_id='envoyer_mail_Début',
        python_callable=envoyer_mail_Début
                                        )
    Envoyer_mail_fin=PythonOperator(
        task_id='envoyer_mail_fin',
        python_callable=envoyer_mail_fin
                                        )
    start >> Envoyer_mail_Debut >> statut_api >> Extraction_donne_meteo >> transformation_donnees_saving >> sauvegarde_donnees >>Envoyer_mail_fin >> final