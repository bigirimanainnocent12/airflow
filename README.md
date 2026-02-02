# **🌤️ Data Pipeline Open Météo**

**📋 Description**

Ce projet implémente une pipeline de données complète utilisant Apache Airflow pour orchestrer la collecte de données météorologiques depuis l'API Open-Meteo, leur traitement avec Pandas, leur stockage dans Cloud sql PostgreSQL, et leur visualisation via un dashboard Power BI.

Le pipeline collecte les données météorologiques en temps réel pour 8 grandes villes du monde :

- Paris (France)
- Berlin (Allemagne)
- Barcelone (Espagne)
- Pretoria (Afrique du Sud)
- Washington DC (États-Unis)
- Beijing (Chine)
- New Delhi (Inde)
- Brasilia (Brésil)

**🏗️ Architecture**
<img width="1137" height="566" alt="image" src="https://github.com/user-attachments/assets/9b65fed6-7613-48aa-a940-bdf87c748e68" />


**Cloud SQL Postgresql**
<img width="1918" height="923" alt="image" src="https://github.com/user-attachments/assets/2de8cd02-7e94-42ef-80be-bb6afbfc44b2" />


**🏗️ Dashboard**

<img width="861" height="686" alt="image" src="https://github.com/user-attachments/assets/8e85cdaa-b65d-466b-b01a-db211a0f6870" />

**Composants principaux :**

* Apache Airflow : Orchestration et automatisation du pipeline
  
* API Open-Meteo : Source de données météorologiques
  
* Pandas : Transformation et nettoyage des données
  
* PostgreSQL : Stockage des données dans le cloud
  
* Power BI : Visualisation et analyse des données
  
**📁 Structure du projet**
<img width="671" height="267" alt="image" src="https://github.com/user-attachments/assets/7f3d9992-2585-4d79-b3fc-ef912382a675" />

**⚙️ Prérequis**

Python 3.12+
Apache Airflow 3.1.5
PostgreSQL (Cloud SQL)
Power BI Desktop

**📈 Fonctionnalités**

- ✅ Collecte automatique des données météo
- ✅ Stockage sécurisé dans le cloud (PostgreSQL)
- ✅ Traitement et nettoyage des données
- ✅ Visualisation interactive avec Power BI
- ✅ Orchestration robuste avec Airflow
- ✅ Gestion des erreurs et retry automatique







