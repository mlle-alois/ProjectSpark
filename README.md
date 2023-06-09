# ProjectSpark
## Lancer le projet

Ouvrez un éditeur, allez dans le répertoire du projet et lancez la commande pour éxécuter
le docker-compose.yml

````bash
docker compose up
````

Dans un autre éditeur dans le répertoire, allez dans le bash du master node

````bash
docker exec -it spark-master bash
````

Puis exécutez le fichier python avec spark-submit

````bash
/spark/bin/spark-submit /app/project-spark-q1.py
````
