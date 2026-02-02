# **🌤️ Data Pipeline Open Météo**

**📋 Description**

Ce projet implémente une pipeline de données complète utilisant Apache Airflow pour orchestrer la collecte de données météorologiques depuis l'API Open-Meteo, leur traitement avec Pandas, leur stockage dans Cloud sql PostgreSQL, et leur visualisation via un dashboard Power BI.

**🏗️ Architecture**
<img width="1146" height="460" alt="image" src="https://github.com/user-attachments/assets/91404b81-13bc-47d4-8e9f-95d1e825d97c" />


**Composants principaux :**

* Apache Airflow : Orchestration et automatisation du pipeline
  
* API Open-Meteo : Source de données météorologiques
  
* Pandas : Transformation et nettoyage des données
  
* PostgreSQL : Stockage des données dans le cloud
  
* Power BI : Visualisation et analyse des données
  
**📁 Structure du projet**
<img width="671" height="267" alt="image" src="https://github.com/user-attachments/assets/7f3d9992-2585-4d79-b3fc-ef912382a675" />


airflow/
├── dags/
│   ├── mesfonctions/
│   │   └── fonctions.py       # Fonctions de traitement des données
│   └── api_meteo.py            # DAG Airflow principal
│── requirements.txt # les dépendances
|── tableau de bord météo.pbix  # Dashboard Power BI
|──météo.png  # Dags pepiline
├── .env                        # Variables d'environnement
└── .gitignore

**⚙️ Prérequis**

Python 3.8+
Apache Airflow 2.0+
PostgreSQL (Cloud SQL)
Power BI Desktop







