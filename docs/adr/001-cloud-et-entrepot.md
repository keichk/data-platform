# ADR 001 — Cloud et entrepôt

## Contexte

Le projet doit construire une plateforme analytique pour un e-commerce à partir de trois sources hétérogènes : une base Postgres de commandes avec environ 20 Go de changements par jour, Une API REST de prestataire de paiement avec quotas et corrections a posteriori, et des logs clickstream d'environ 30 Go par jour avec évolution de schéma.

Les besoins principaux sont :
- des dashboards métier disponibles avant 7h chaque matin ;
- une table de features quotidienne pour un modèle de churn ;
- 3 ans d'historique ;
- la suppression de données d'un client sur demande dans le cadre RGPD ;
- un budget cloud sous surveillance.

La plateforme doit donc supporter des charges analytiques importantes, des traitements SQL sur historique, des relances partielles, ainsi qu'une exploitation limitée grâce à des services managés. BigQuery estprésenté comme un entrepôt analytique entièrement managé et  serverless, adapté aux analyses SQL à  grande échelle.

## Décision

Le projet retient **GCP** comme cloud principal et **BigQuery** comme entrepôt analytique central.

Les données brutes sont déposées dans **Cloud Storage** puis chargées dans **BigQuery** pur les couches bronze, silver et gold. L'orchestration est assurée par **Airflow via Cloud Composer**, et les transformations analytiquessont gérées par **dbt** dans BigQuery. Cloud Composer est un service managé par Apache Airflow, adapté à l'orchestration de workflows multi-étapes sur GCP.

Ce choix est motivé par : 
- un entrepôt serverless limitant l'administration ;
- une bonne adéquation entre SQL analytique, historique long et agrégations métier ;
- une intégration naturelle entre Cloud Storage, BigQuery, Airflow et dbt ;
- une capacité à absorber des volumes journaliers imporants sans concevoir une plateforme distribuée plus lourde dès la première tranche.

## Alternatives écartées

- **AWS + Redshift** : solution valable, mais non retenue car la stack cible du projet est orientée GCP et l'intégration native recherchée est plus directe avec BigQuery et Cloud Composer.
- **Snowflake** : très bon entrepôt analytique, mais écarté pour éviter d'ajouter une couche fournisseur supplémentaire alors que GCP fournit déjà un entrepôt analytique managé repondant au besoin
- **PostgreSQL comme entrepôt analytique** : insuffisant à moyen terme pour absorber efficacement les usages analytiques, l'historisation sur 3 ans et les agrégations de logs clickstream à ce volume.
- **Spark + data lake comme socle principal** : trop complexe pour la tranche réduite ; cette option augmente l'effort d'exploitation et d'industrialisation alors que le besoin principal est une plateforme batch analytique disponible avant 7h.
- **Architecture très orientée streaming** : non prioritaire, car le besoin metier impose une fraîcheur quotidienne avant 7h et non du temps réel. 

## Conséquences

Le projet gagne une plateforme analytique cohérente, majoritairement managée, et bien adaptée à un fonctionnement batch nocturne avec stockage historique et exposition BI.

En Contrepartie, l'arhitecture devient dépendante de GCP et des patterns BigQuery, notamment pour les stratégies de partitionnement, les coûts de requêtes et les mécanismes de chargement incrémental.

Le choix de BigQuery et dbt favorise aussi un modèle ELT dans lequel les données sont chargées rapidement puis transformées dans l'entrepôt, avec tests et documentation intégrés.