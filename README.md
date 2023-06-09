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

Ajouter numpy pour pouvoir effectuer la question 4

````bash
apk add --update python python-dev gfortran py-pip build-base py-numpy sur le master
````

Puis exécutez le fichier python avec spark-submit

````bash
/spark/bin/spark-submit /app/project-spark.py
````
