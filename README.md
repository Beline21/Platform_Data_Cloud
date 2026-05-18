# Construction d'une plateforme ELT



## Projet effectué par Coline Besson dans le cadre d'un cours de Plateformes de données sur le Cloud mené par Mr Jérémy Gros.

Ce projet a débuté le 12 février 2026 et doit se finir le 15 juin 2026.

Il a pour objectif de consuruire une platteforme ELT en utilisant les outils tels que Docker, pgAdmin, Apache Airflow, Portainer, Snowflake, Terraform et DBT.

Vous devrez avoir installé les applications suivantes : Docker, pgAdmin, Apache Airflow, Portainer, Terraform, dbt, PostgreSQL et Git.


Et vous devrez créer un compte à ces applications : Docker, Snowflake, GitHub et dbt.



## Ce projet a été divisé en séances :

1. Docker a été configuré pour que toutes les données vivent sous /opt. Nous avons mis en place le réseau Docker, PostgreSQL (entrepôt), Redis (pour Airflow plus tard) et pgAdmin.

2. Apache Airflow a été installé en s’appuyant sur le PostgreSQL (et Redis). Une chaîne Git -> CI -> DAG Bundle a été mise en place pour livrer les DAGs. Les premiers DAGs d’extraction métier ont été réalisés : DVF 2025 (fichier) et Open-Meteo (API). La gestion des échecs avec retries et notification (avec ntfy) a été mise en place. Les DAGs se nomment download_dvf_2025_dag.py et download_open_meteo_dag.py.

3. Les schémas bronze, silver, gold ont été créés dans Postgres. La construction d'un DAG ELT complet dans Airflow a été fait : extraction → chargement bronze → transformation SQL vers silver.

4. Un projet dbt avec l’approche medaillon a été mis en place : dossiers models/bronze/, models/silver/, models/gold/ et schémas PostgreSQL bronze, silver, gold. Nous avons construit les modèles silver (nettoyage, typage) et gold (marts). Pour DVF, un schéma en étoile a été visé : table de fait (mutations) + dimensions (commune, type de local, nature mutation, temps).

5. Un flux E2E a été construit : extraction (E) → chargement en bronze (L) → exécution dbt (silver → gold). Tout orchestré par un seul DAG Airflow nommé e2e_dag.py. La plateforme a été sécurisée : rôles Airflow (RBAC) et gestion des secrets (Variables / Connections, plus de mots de passe en dur dans le code). A travers Airflow, il est donc possible de lancer le DAG e2e_dag.py qui va se charger d'extraire, charger et transformer les données dans respectivement les buckets bronze, dbt_silver et dbt_gold observables dans PgAdmin.

6. Un environnement Snowflake a été provisionné via Terraform (Infrastructure as Code) : warehouse, database, schémas (bronze, silver, gold), rôles, grants, stages.

7. Un lac de données a été mis en place dans Snowflake via les stages internes (raw, fichiers partitionnés). Le flux E → L → T a été reproduit sur Snowflake : extraction, dépôt dans le stage raw, COPY INTO bronze, dbt silver → gold. L'ensemble a été orchestré avec Airflow (connexion Snowflake, DAG dédié).



## Pour lancer ce projet, vous aurez besoin de :

0. Créer une Machine virtuelle et installer les dépendances :
cd ~/platform-data
python3 -m venv .venv
source .venv/bin/activate
pip install apache-airflow
pip install dbt-postgres
pip install pandas requests sqlalchemy psycopg2-binary
pip install apache-airflow-providers-postgres
pip install apache-airflow-providers-snowflake
pip install snowflake-connector-python
pip install astronomer-cosmos

1. Lancer Docker à la racine :
docker compose up -d

2. Initialiser Airflow :
cd ~/platform-data/airflow
docker compose up airflow-init
docker compose up -d

3. Accéder aux interfaces grâce aux sites :
pgAdmin : http://localhost:5050
Airflow : http://localhost:8080
Portainer : https://localhost:9443/

4. Création des schémas PostgreSQL :
docker exec -it postgres psql -U postgres
Puis dans PostgreSQL :
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;

5. Exécution des DAGs Airflow sur le site ou par commande :
cd ~/platform-data/airflow/dags/
docker exec -it airflow-scheduler airflow dags trigger e2e_dag

6. Exécution de dbt :
cd ~/platform-data/dbt_models/platform_dbt
dbt run
dbt test # Tests
dbt docs generate # Documentation
dbt docs serve # Documentation

7. Exécution de Terraform (Snowflake) :
terraform init
terraform validate
terraform plan
terraform apply

8. Arrêt de la plateforme :
docker compose down
docker compose down -v



Une fois toutes les configurations faites, un démarrage rapide se fait en faisant :
cd platform-data
source .venv/bin/activate
docker compose up -d
cd airflow
docker compose up -d
cd ../dbt_models/platform_dbt
dbt run





## La structure du dossier contenant le projet est la suivante :

|-  platform-data
   |-  .venv
   |-  README.md
   |-  docker-compose.yml
   |-  dags
   |-  dbt_models
      |-  logs
      |-  platform_dbt
         |-  models
            |-  sources.yml
            |-  silver
               |-  dvf_mutations.sql
               |-  meteo_quotidien.sql
               |-  schema.yml
            |-  gold
               |-  dim_commune.sql
               |-  dim_type_local.sql
               |-  fact_mutations.sql
               |-  fact_meteo.sql  
               |-  schema.yml
         |-  target
            |-  catalog.json
         |-  dbt_project.yml
   |-  .github
      |-  workflows
         |-  ci-dags.yml
   |-  airflow
      |-  config
      |-  dags
         |-  hello_two_dag.py
         |-  download_dvf_2025_dag.py
         |-  download_open_meteo_dag.py
         |-  
         |-  utils
             |-  notifications.py
      |-  data
         |-  airflow
      |-  docker-compose.yaml
      |-  logs
      |-  plugins
      |-  .env
