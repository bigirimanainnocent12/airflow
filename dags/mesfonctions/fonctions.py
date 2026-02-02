from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import datetime, timedelta
from airflow.utils.email import send_email
import json
nomscolonnes={"temperature_2m":"temperature_2m",
                  "relative_humidity_2m":"humidite_relative_2m",
                  "apparent_temperature":"temperature_apparente",
                  "is_day":"est_jour",
                  "wind_speed_10m":"vitesse_vent_10m",
                  "wind_direction_10m":"direction_vent_10m",
                  "wind_gusts_10m":"rafales_vent_10m",
                  "precipitation":"precipitations",
                  "rain":"pluie",
                  "showers":"averses",
                  "snowfall":"chutes_neige",
                  "weather_code":"code_meteo",
                  "cloud_cover":"couverture_nuageuse",
                  "pressure_msl":"pression_niveau_mer",
                  "surface_pressure":"pression_surface",
                  "time":"temps"}


weather_descriptions = {
        0: 'Ciel dégagé',
        1: 'Principalement dégagé',
        2: 'Partiellement nuageux',
        3: 'Couvert',
        45: 'Brouillard',
        48: 'Brouillard givrant',
        51: 'Bruine légère',
        53: 'Bruine modérée',
        55: 'Bruine dense',
        61: 'Pluie légère',
        63: 'Pluie modérée',
        65: 'Pluie forte',
        71: 'Neige légère',
        73: 'Neige modérée',
        75: 'Neige forte',
        95: 'Orage',
    }


def create_table_meteo():
    """Fonction pour créer la table meteo_previsions si elle n'existe pas"""
    hook = PostgresHook(postgres_conn_id="api_mete_cloud")
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS meteo_previsions (
            timestamp VARCHAR,
            date_horaire DATE,
            hour INT,
            temperature_2m FLOAT,
            humidite_relative_2m FLOAT,
            temperature_apparente FLOAT,
            est_jour INT,
            vitesse_vent_10m FLOAT,
            direction_vent_10m FLOAT,
            rafales_vent_10m FLOAT,
            precipitations FLOAT,
            pluie FLOAT,
            averses FLOAT,
            chutes_neige FLOAT,
            code_meteo INT,
            couverture_nuageuse FLOAT,
            pression_niveau_mer FLOAT,
            pression_surface FLOAT,
            ville TEXT,
            latitude FLOAT,
            longitude FLOAT,
            weather_description TEXT
        );
    """)

    cur.execute("""
                
        CREATE TABLE IF NOT EXISTS meteo (
            timestamp VARCHAR,
            date_horaire DATE,
            hour INT,
            temperature_2m FLOAT,
            humidite_relative_2m FLOAT,
            temperature_apparente FLOAT,
            est_jour INT,
            vitesse_vent_10m FLOAT,
            direction_vent_10m FLOAT,
            rafales_vent_10m FLOAT,
            precipitations FLOAT,
            pluie FLOAT,
            averses FLOAT,
            chutes_neige FLOAT,
            code_meteo INT,
            couverture_nuageuse FLOAT,
            pression_niveau_mer FLOAT,
            pression_surface FLOAT,
            ville TEXT,
            latitude FLOAT,
            longitude FLOAT,
            weather_description TEXT
        );
                
    """)


    conn.commit()
    cur.close()
    conn.close()



def cordonnees():
    """Fonction pour obtenir les coordonnées de plusieurs villes""" 
    cordonnees = {
        "ville":["Paris","Berlin","Barcelonne","Pretoria","Washington DC","Beijing","New Delhi","Brasilia"],
        "latitude":[48.8534, 52.5244, 41.3888, -25.7449, 38.8951, 39.9075, 28.6358, -15.7797],
        "longitude": [2.3488, 13.4105, 2.1590, 28.1878, -77.0364, 116.3972, 77.2245, -47.9297]
                }
    data_cordonnees=pd.DataFrame(cordonnees)
    return data_cordonnees

def extraction_donnees_meteo_current(**context):

    """Fonction pour extraire les données météo actuelles depuis l'API Open-Meteo""" 
    ti = context['ti']
    data_cordonnees= cordonnees()
    data= []

    for i in  range(len(data_cordonnees)):
        LATITUDE = data_cordonnees.iloc[i]['latitude']
        LONGITUDE = data_cordonnees.iloc[i]['longitude']
        Ville= data_cordonnees.iloc[i]['ville']
        # Construction de l'endpoint
        endpoint = f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=Europe/Paris"
        http_hook = HttpHook(http_conn_id='open_meteo_api', method='GET')
        response = http_hook.run(endpoint)
        weather_data = response.json()
        current = weather_data.get('current', {}) 
        current['ville']= Ville
        current['latitude']= LATITUDE
        current['longitude']= LONGITUDE
        data.append(current)
    data=pd.DataFrame(data).to_dict('records')    

    ti.xcom_push(key='weather_data', value=data)
    return data


def extraction_donnees_meteo_previsions(**context):
    ti = context['ti']
    data_cordonnees = cordonnees()
    all_rows = []

    for i in range(len(data_cordonnees)):
        LATITUDE = data_cordonnees.iloc[i]['latitude']
        LONGITUDE = data_cordonnees.iloc[i]['longitude']
        VILLE = data_cordonnees.iloc[i]['ville']

        endpoint = (
            f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}"
            "&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,"
            "is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,"
            "pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,"
            "wind_gusts_10m&timezone=Europe/Paris"
        )

        http_hook = HttpHook(http_conn_id='open_meteo_api', method='GET')
        response = http_hook.run(endpoint)
        weather = response.json()

        hourly = weather.get("hourly", {})
        keys = hourly.keys()
        n = len(hourly.get("time", []))

        for idx in range(n):
            row = {k: hourly[k][idx] for k in keys}
            row["ville"] = VILLE
            row["latitude"] = LATITUDE
            row["longitude"] = LONGITUDE
            all_rows.append(row)

    ti.xcom_push(key='weather_data_previsions', value=all_rows)
    return all_rows



def transformation_donnees_meteo(**context):
    ti = context['ti']
    data = ti.xcom_pull(key='weather_data', task_ids='Extraction_donne_meteo.extraction_donnees_meteo')
    df= pd.DataFrame(data)
 
    transformed_data= df.rename(columns=nomscolonnes)
    transformed_data['timestamp'] = pd.to_datetime(transformed_data['temps'])
    transformed_data['date'] = transformed_data['timestamp'].dt.date.astype(str)
    transformed_data['hour'] = transformed_data['timestamp'].dt.hour.astype(int)

    transformed_data['timestamp'] = transformed_data['timestamp'].astype(str)
    transformed_data['date'] = transformed_data['date'].astype(str)
    transformed_data['hour'] = transformed_data['hour'].astype(int)
    transformed_data.drop(columns=['temps'], inplace=True)

    transformed_data['weather_description'] = transformed_data['code_meteo'].map(lambda x: weather_descriptions.get(x, 'Inconnu'))
    transformed_data= transformed_data.to_dict('records')
    ti.xcom_push(key='transformed_data', value=transformed_data)
    
    return transformed_data

def transformation_donnees_meteo_previsions(**context):
    ti = context['ti']

    
    data = ti.xcom_pull(key='weather_data_previsions',task_ids='Extraction_donne_meteo.extraction_donnees_meteo_previsions'
                         )
    df = pd.DataFrame(data) 

    df = df.rename(columns=nomscolonnes)
    df["timestamp"] = pd.to_datetime(df["temps"])    
    df["date"] = df["timestamp"].dt.date.astype(str)
    df["hour"] = df["timestamp"].dt.hour.astype(int)   


    df['timestamp'] = df['timestamp'].astype(str)
    df['date'] = df['date'].astype(str)
    df['hour'] = df['hour'].astype(int)
    df.drop(columns=["temps"], inplace=True)

  
    df["weather_description"] = df["code_meteo"].map(
        lambda x: weather_descriptions.get(x, "Inconnu")
    )
    transformed_data_previsions = df.to_dict("records")

    ti.xcom_push(
        key="transformed_data_previsions",value=transformed_data_previsions
                )

    return transformed_data_previsions



def sauvegarder_data(**context):
    ti = context["ti"]

    # 1. Récupération des données transformées depuis XCom
    data = ti.xcom_pull(
        task_ids="transformation_donnees.transform_weather_current",
        key="transformed_data"
    )

    # Vérification : data doit être une liste de dictionnaires
    if isinstance(data, dict):
        data = [data]

    df = pd.DataFrame(data)

    # 2. Connexion PostgreSQL via Airflow
    hook = PostgresHook(postgres_conn_id="api_mete_cloud")
    conn = hook.get_conn()
    cur = conn.cursor()

    # 3. Création de la table
    
    # 4. Requête d’insertion
    insert_query = """
        INSERT INTO meteo(
            timestamp,date_horaire,hour, temperature_2m, humidite_relative_2m, temperature_apparente, est_jour,
            vitesse_vent_10m, direction_vent_10m, rafales_vent_10m, precipitations,
            pluie, averses, chutes_neige, code_meteo, couverture_nuageuse,
            pression_niveau_mer, pression_surface, ville, latitude,
            longitude, weather_description
        )
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 5. Insertion ligne par ligne
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("timestamp"),
            row.get("date"),
            row.get("hour"),
            row.get("temperature_2m"),
            row.get("humidite_relative_2m"),
            row.get("temperature_apparente"),
            row.get("est_jour"),
            row.get("vitesse_vent_10m"),
            row.get("direction_vent_10m"),
            row.get("rafales_vent_10m"),
            row.get("precipitations"),
            row.get("pluie"),
            row.get("averses"),
            row.get("chutes_neige"),
            row.get("code_meteo"),
            row.get("couverture_nuageuse"),
            row.get("pression_niveau_mer"),
            row.get("pression_surface"),
            row.get("ville"),
            row.get("latitude"),
            row.get("longitude"),
            row.get("weather_description")
        ))

    conn.commit()
    cur.close()
    conn.close()







def sauvegarder_data_previsions(**context):
    # Récupération des données transformées depuis XCom
    data = context["ti"].xcom_pull(task_ids="transformation_donnees.transform_weather_previsions", key="transformed_data_previsions")
    max_date = context["ti"].xcom_pull(task_ids="sauvegarde_donnees.get_max_date")[0][0] 
    print("Date max trouvée :", max_date)
    df = pd.DataFrame(data)
    df['date_horaire'] = pd.to_datetime(df['date'])
    if max_date is None:
        print("Table vide → chargement complet")
    # pas de filtre
    else:
     max_date = pd.to_datetime(max_date)
     df = df[df['date_horaire'] > max_date]

    df.drop(columns=['date'], inplace=True)
    # Connexion PostgreSQL via Airflow
    hook = PostgresHook(postgres_conn_id="api_mete_cloud")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Requête d'insertion
    insert_query = """
        INSERT INTO meteo_previsions(
            timestamp,date_horaire,hour, temperature_2m, humidite_relative_2m, temperature_apparente, est_jour,
            vitesse_vent_10m, direction_vent_10m, rafales_vent_10m, precipitations,
            pluie, averses, chutes_neige, code_meteo, couverture_nuageuse,
            pression_niveau_mer, pression_surface, ville, latitude,
            longitude, weather_description
        )
        VALUES ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """ 
    # Insertion ligne par ligne
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("timestamp"),
            row.get("date_horaire"),
            row.get("hour"),
            row.get("temperature_2m"),
            row.get("humidite_relative_2m"),
            row.get("temperature_apparente"),
            row.get("est_jour"),
            row.get("vitesse_vent_10m"),
            row.get("direction_vent_10m"),
            row.get("rafales_vent_10m"),
            row.get("precipitations"),
            row.get("pluie"),
            row.get("averses"),
            row.get("chutes_neige"),
            row.get("code_meteo"),
            row.get("couverture_nuageuse"),
            row.get("pression_niveau_mer"),
            row.get("pression_surface"),
            row.get("ville"),
            row.get("latitude"),
            row.get("longitude"),
            row.get("weather_description")
        ))
    conn.commit()
    cur.close()
    conn.close()

def envoyer_mail_Début():
    html=f"""
    <h2>Début du pipeline météo</h2>
    <p>Le Worflow météo s'est exécuté avec succès le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>
    <p>Cordialement,<br>Votre Airflow Workflow</p> 
    """
    send_email(
        to=["gatoziinnocent@gmail.com", "bigirimanainnocent596@gmail.com"],
        subject="Début Statut Workflow Airflow Météo",
        html_content=html
        )

    return "Email envoyé avec succès"

def envoyer_mail_fin():
    html=f"""
    <h2>Statut du pipeline météo</h2>
    <p>Le Worflow météo s'est bien exécuté avec succès le {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>
    <ul>
        <li><b>Extraction des données météo actuelles et des prévisions :</b> Réussie</li>
        <li><b>Transformation des données :</b> Réussie</li>
        <li><b>Sauvegarde des données dans PostgreSQL :</b> Réussie</li>
    </ul>

    <p>Cordialement,<br>Votre Airflow Workflow</p> 
    """
    send_email(
        to=["gatoziinnocent@gmail.com", "bigirimanainnocent596@gmail.com"],
        subject="Fin Statut Workflow Airflow Météo",
        html_content=html
        )

    return "Email envoyé avec succès"