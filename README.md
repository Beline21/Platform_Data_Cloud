Welcome in the project Plateform Data !

Premices :
You will need to have Docker installed in your computer or VM.

Let's start with basics commands:
1. Create network to enable the services to communicate :
     sudo docker network create platform-net
2. Launch Docker:
     sudo docker compose up -d


All the files and documents are organised this way :
/opt
|-  containerd
|-  lost+found
|-  docker
   |-  daemon.json
|-  airflow
   |-  output 
|-  docker-volumes
   |-  postgres
   |-  redis
   |-  pgadmin
   |-  airflow
      |-  logs
      |-  config
      |-  plugins
      |-  data
      |-  dags
         |-  hello_dag.py

~ 
|-  platform-data
   |-  README.md
   |-  docker-compose.yml
   |-  dags
   |-  airflow
      |-  config
      |-  dags
         |-  hello_two_dag.py
      |-  data
         |-  airflow
      |-  docker-compose.yaml
      |-  logs
      |-  plugins
      |-  .env
