# Construction d'une plateforme de données ELT sur le Cloud

## Projet réalisé par Coline Besson dans le cadre du cours « Plateformes de données sur le Cloud » dispensé par M. Jérémy Gros

Ce projet a été réalisé entre le 12 février 2026 et le 15 juin 2026.

L'objectif est de concevoir et déployer une plateforme de données ELT (*Extract, Load, Transform*) reposant sur des technologies modernes de Data Engineering.

Les outils utilisés sont les suivants :

* Docker
* PostgreSQL
* pgAdmin
* Apache Airflow
* Portainer
* dbt (Data Build Tool)
* Snowflake
* Terraform
* Git et GitHub

Le dépôt GitHub du projet est disponible à l'adresse suivante :

https://github.com/Beline21/Platform_Data_Cloud.git

Les captures d'écran illustrant les différentes étapes du projet sont disponibles dans le répertoire `results/`.

---

# Prérequis

## Logiciels à installer

* Docker
* PostgreSQL
* pgAdmin
* Git
* Python 3
* Terraform

## Comptes à créer

* Docker Hub
* GitHub
* Snowflake
* dbt Cloud (optionnel)

---

# Présentation de l'architecture

La plateforme suit une architecture **médaillon** organisée autour de trois couches :

## Bronze

Zone de stockage des données brutes extraites depuis les différentes sources.

## Silver

Zone de nettoyage, normalisation et typage des données.

## Gold

Zone de restitution analytique contenant les tables métier et les modèles décisionnels.

Deux sources de données sont exploitées :

### DVF (Demandes de Valeurs Foncières)

Jeu de données public recensant les mutations immobilières françaises.

### Open-Meteo

API météorologique permettant de récupérer des données climatiques quotidiennes.

L'orchestration de l'ensemble des traitements est assurée par Apache Airflow.

Les transformations sont réalisées avec dbt.

---

# Schéma global du projet

L'organisation globale du projet est la suivante :

```text
platform-data
├── .venv
├── README.md
├── docker-compose.yml
├── .github
│   └── workflows
│       └── ci-dags.yml
│
├── airflow
│   ├── dags
│   │   ├── hello_two_dag.py
│   │   ├── download_dvf_2025_dag.py
│   │   ├── download_open_meteo_dag.py
│   │   ├── e2e_dag.py
│   │   ├── elt_snowflake.py
│   │   └── utils
│   │       └── notifications.py
│   ├── data
│   │   └── airflow
│   │       ├── dvf_2025.csv
│   │       └── open_meteo_berlin.json
│   └── docker-compose.yaml
│
├── dbt_models
│   └── platform_dbt
│       ├── models
│       │   ├── sources.yml
│       │   ├── silver
│       │   │   ├── dvf_mutations.sql
│       │   │   ├── meteo_quotidien.sql
│       │   │   └── schema.yml
│       │   └── gold
│       │       ├── dim_commune.sql
│       │       ├── dim_type_local.sql
│       │       ├── fact_mutations.sql
│       │       ├── fact_meteo.sql
│       │       └── schema.yml
│       ├── target
│       │   └── catalog.json
│       └── dbt_project.yml
│
└── terraform
    └── snowflake
        ├── main.tf
        ├── variables.tf
        ├── terraform.tfvars
        └── versions.tf
```

## Rôle de chaque composant

### Docker

Docker permet de déployer et d'isoler l'ensemble des services de la plateforme :

* PostgreSQL ;
* Redis ;
* pgAdmin ;
* Portainer ;
* Apache Airflow.

### Apache Airflow

Airflow orchestre les traitements de bout en bout :

* extraction des données DVF ;
* extraction des données Open-Meteo ;
* chargement des données ;
* exécution des transformations dbt ;
* chargement dans Snowflake.

Les principaux DAGs sont :

| DAG                          | Fonction                           |
| ---------------------------- | ---------------------------------- |
| `download_dvf_2025_dag.py`   | Extraction du fichier DVF          |
| `download_open_meteo_dag.py` | Extraction depuis l'API Open-Meteo |
| `e2e_dag.py`                 | Pipeline ELT PostgreSQL complet    |
| `elt_snowflake.py`           | Pipeline ELT Snowflake             |

### dbt

dbt réalise les transformations métier selon l'architecture médaillon :

```text
Bronze
   ↓
Silver
   ↓
Gold
```

Les modèles Silver assurent le nettoyage et la standardisation des données.

Les modèles Gold construisent les tables analytiques :

* FACT_MUTATIONS ;
* FACT_METEO ;
* DIM_COMMUNE ;
* DIM_TYPE_LOCAL ;
* DIM_TIME.

### Terraform

Terraform permet le provisionnement automatique de l'infrastructure Snowflake :

* Warehouse ;
* Database ;
* Schémas ;
* Stages ;
* Rôles ;
* Permissions.

### Snowflake

Snowflake héberge le Data Lake et les couches analytiques du projet.

Le flux de traitement est le suivant :

```text
DVF / Open-Meteo
        ↓
     Airflow
        ↓
    RAW_STAGE
        ↓
   COPY INTO
        ↓
     BRONZE
        ↓
      dbt
        ↓
     SILVER
        ↓
      GOLD
```

### GitHub

GitHub assure :

* le versionnement du projet ;
* la collaboration ;
* l'intégration continue via GitHub Actions.

```
```

---

# Déroulement du projet

## Séance 1 : Mise en place de l'infrastructure Docker

Docker a été configuré afin de stocker les données persistantes sous `/opt`.

Les services suivants ont été déployés :

* PostgreSQL
* Redis
* pgAdmin
* Portainer

---

## Séance 2 : Déploiement d'Apache Airflow

Apache Airflow a été installé en s'appuyant sur PostgreSQL et Redis.

Une chaîne GitHub → CI → DAG Bundle a été mise en place afin d'automatiser le déploiement des DAGs.

Deux DAGs d'extraction ont été développés :

* `download_dvf_2025_dag.py`
* `download_open_meteo_dag.py`

Les fonctionnalités suivantes ont également été mises en œuvre :

* gestion des erreurs ;
* mécanismes de relance (*retries*) ;
* notifications via ntfy (non visibles sur les captures d'écran car les erreurs se sont effacées au bout d'une semaine).

---

## Séance 3 : Construction du pipeline ELT PostgreSQL

Les schémas suivants ont été créés dans PostgreSQL :

```text
bronze
silver
gold
```

Un pipeline ELT a été développé dans Airflow afin de réaliser :

```text
Extraction
    ↓
Chargement Bronze
    ↓
Transformation Silver
```

Les données brutes sont chargées dans Bronze avant d'être nettoyées et transformées dans Silver.

---

## Séance 4 : Mise en place des transformations avec dbt

Un projet dbt a été développé selon l'approche médaillon.

Les données Bronze sont déclarées comme sources dans dbt.

Les transformations sont réalisées dans les couches :

```text
models/
├── silver/
└── gold/
```

### Couche Silver

Les modèles Silver assurent :

* le nettoyage ;
* le typage ;
* la normalisation des données.

### Couche Gold

Les modèles Gold fournissent des tables analytiques prêtes à être exploitées.

Pour les données DVF, un schéma en étoile a été construit comprenant :

#### Table de faits

* FACT_MUTATIONS
* FACT_METEO

#### Dimensions

* DIM_COMMUNE
* DIM_TYPE_LOCAL
* DIM_TIME

---

## Séance 5 : Pipeline ELT de bout en bout

L'ensemble du processus a été orchestré par un DAG unique :

```text
e2e_dag.py
```

Le pipeline suit le flux :

```text
Extraction
    ↓
Bronze
    ↓
dbt Silver
    ↓
dbt Gold
```

La plateforme a été sécurisée grâce à :

* la gestion des rôles Airflow (RBAC) ;
* l'utilisation des Variables Airflow ;
* l'utilisation des Connections Airflow ;
* la suppression des identifiants présents dans le code.

Depuis Airflow, il est possible d'exécuter le DAG `e2e_dag.py` afin d'automatiser l'ensemble du traitement et d'observer le résultat dans pgAdmin.

---

## Séance 6 : Provisionnement de Snowflake avec Terraform

Une infrastructure Snowflake a été déployée selon une approche Infrastructure as Code (IaC).

Les ressources suivantes sont créées automatiquement :

* Warehouse
* Database
* Schéma Bronze
* Schéma Silver
* Schéma Gold
* Rôles
* Grants
* Stages

---

## Séance 7 : Reproduction du pipeline dans Snowflake

Le pipeline ELT a été reproduit dans Snowflake.

Les fichiers sont déposés dans des stages internes avant leur chargement dans les tables Bronze.

Le flux mis en œuvre est le suivant :

```text
Extraction
    ↓
RAW_STAGE
    ↓
COPY INTO Bronze
    ↓
Silver
    ↓
Gold
```

L'orchestration est assurée par Airflow à travers un DAG dédié `elt_snowflake.py`.

---

# Installation et démarrage

## 1. Création de l'environnement Python

```bash
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
```

---

## 2. Démarrage de Docker

Depuis la racine du projet :

```bash
docker compose up -d
```

---

## 3. Initialisation d'Airflow

```bash
cd ~/platform-data/airflow

docker compose up airflow-init
docker compose up -d
```

---

## 4. Création des schémas PostgreSQL

Connexion :

```bash
docker exec -it postgres psql -U postgres
```

Puis :

```sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

---

## 5. Exécution des DAGs Airflow

Depuis l'interface Airflow ou via la ligne de commande :

```bash
cd ~/platform-data/airflow/dags/
docker exec -it airflow-scheduler airflow dags trigger e2e_dag
```

---

## 6. Exécution des modèles dbt

```bash
cd ~/platform-data/dbt_models/platform_dbt

dbt run --target snowflake
dbt test --target snowflake

dbt docs generate --target snowflake
dbt docs serve
```

---

## 7. Déploiement Snowflake avec Terraform

```bash
terraform init
terraform validate
terraform plan
terraform apply
```

---

## 8. Arrêt de la plateforme

```bash
docker compose down
docker compose down -v
```

---

# Démarrage rapide

```bash
cd ~/platform-data

source .venv/bin/activate

docker compose up -d

cd airflow
docker compose up -d

cd dbt_models/platform_dbt

dbt run --target snowflake
```

---

# Gouvernance et sécurité

Aucun mot de passe, jeton d'accès ou clé d'API n'est stocké dans le dépôt Git.

Les secrets sont gérés par Apache Airflow via :

* les Variables ;
* les Connections.

Les informations sensibles sont récupérées dynamiquement dans le code grâce à :

```python
Variable.get(...)
BaseHook.get_connection(...)
```

Cette approche permet de séparer la configuration de l'implémentation applicative.

---

# Organisation du stockage

## Stockage local

Les données persistantes sont stockées dans `/opt`.

```text
/opt
├── docker
├── docker-volumes
│   ├── postgres
│   ├── redis
│   ├── pgadmin
│   └── airflow
│       ├── logs
│       ├── data
│       ├── dags
|       |   └── hello_dag.py
│       ├── config
|       |   └── airflow.cfg
│       └── plugins
└── airflow
    └── output
```

---

# Organisation du Data Lake Snowflake

Les fichiers sont chargés depuis Airflow vers `RAW_STAGE`.

Ils sont ensuite intégrés dans les tables Bronze à l'aide de commandes `COPY INTO`.

Les données sont transformées dans Silver puis modélisées dans Gold.

```text
PLATFORM_DB
├── BRONZE
│   ├── Tables
│   │   ├── DVF_MUTATIONS
│   │   └── METEO_QUOTIDIEN
│   └── Stages
│       ├── RAW_STAGE
│       │   ├── dvf
│       │   │   └── annee=2025
│       │   │       └── dvf_2025.csv
│       │   └── meteo
│       │       └── date=2026-06-28
│       │           └── open_meteo_berlin.json
│       └── REFINED_STAGE
│
├── SILVER_SILVER
│   └── Tables
│       ├── DVF_MUTATIONS
│       └── METEO_QUOTIDIEN
│
└── SILVER_GOLD
    └── Tables
        ├── DIM_COMMUNE
        ├── DIM_TIME
        ├── DIM_TYPE_LOCAL
        ├── FACT_METEO
        └── FACT_MUTATIONS
```

---

# Résultats obtenus

La plateforme permet :

* l'extraction automatisée de données issues de fichiers et d'API ;
* l'orchestration complète des traitements avec Apache Airflow ;
* la transformation des données avec dbt ;
* le chargement dans PostgreSQL et Snowflake ;
* la gestion sécurisée des secrets ;
* le provisionnement automatique de l'infrastructure Snowflake avec Terraform ;
* la mise à disposition de modèles analytiques exploitables dans la couche Gold.
