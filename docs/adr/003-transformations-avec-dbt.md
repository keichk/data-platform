# ADR 003 — Transformations avec dbt

## Contexte

La plateforme data reçoit des données hétérogènes provenant de plusieurs sources :

- une base Postgres contenant les commandes ;
- une API REST de prestataire de paiement ;
- des logs JSON de clickstream avec un schéma évolutif.

Les données doivent être transformées afin de produire :

- des tables propres pour les commandes, les paiements et les clients ;
- des indicateurs métier pour les dashboards ;
- une table quotidienne de features destinée au modèle de churn ;
- des données historisées sur 3 ans ;
- des données contrôlées avant leur publication aux utilisateurs.

Les transformations doivent être versionnées avec le code, reproductibles, testables et compréhensibles par une autre personne de l’équipe. Elles doivent également pouvoir fonctionner en mode incrémental pour éviter de retraiter inutilement les volumes historiques.

Sans outil dédié, les transformations pourraient être réparties entre des scripts Python, des requêtes SQL lancées par Airflow et des procédures directement exécutées dans BigQuery. Cette organisation rendrait les dépendances, les tests et la documentation plus difficiles à maintenir.

## Décision

Le projet utilise **dbt avec BigQuery** pour les transformations analytiques et les contrôles de qualité des données.

dbt est organisé en trois niveaux :

```text
Sources / Bronze
        ↓
Staging
        ↓
Silver
        ↓
Gold
```

### Modèles staging

Les modèles staging servent à rapprocher les données de leur source tout en les standardisant.

Ils peuvent notamment :

- renommer les colonnes ;
- convertir les types ;
- normaliser les statuts ;
- ajouter les métadonnées techniques ;
- supprimer les colonnes inutiles ;
- définir les sources utilisées par dbt.

Exemples :

```text
stg_orders
stg_payments
stg_clickstream
```

### Modèles silver

Les modèles silver contiennent des données nettoyées et dédupliquées.

Ils peuvent notamment :

- appliquer les clés métier ;
- gérer les doublons ;
- harmoniser les statuts de commande et de paiement ;
- contrôler les montants ;
- relier les commandes aux clients ;
- traiter les corrections tardives des paiements.

Exemples :

```text
fct_orders
fct_payments
fct_clickstream_events
dim_customers
```

### Modèles gold

Les modèles gold sont orientés vers les usages métier et data science.

Exemples :

```text
daily_sales
daily_payment_metrics
customer_activity
churn_features_daily
```

Ces modèles sont consommés par les dashboards et les traitements de machine learning.

### Tests dbt

Les tests dbt sont utilisés pour empêcher la publication de données invalides. Les tests vérifient notamment :

- qu’une clé n’est pas nulle ;
- qu’une clé est unique ;
- qu’une valeur appartient à une liste autorisée ;
- qu’une relation entre deux tables est valide ;
- qu’un montant est cohérent ;
- qu’une date se trouve dans une plage attendue.

Un test dbt est considéré comme réussi lorsqu’il ne retourne aucune ligne en erreur.

Exemple :

```yaml
version: 2

models:
  - name: fct_orders
    description: "Commandes nettoyées et dédupliquées."

    columns:
      - name: order_id
        description: "Identifiant unique de la commande."
        data_tests:
          - not_null
          - unique

      - name: customer_id
        description: "Identifiant du client."
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id

      - name: order_status
        data_tests:
          - accepted_values:
              arguments:
                values:
                  - pending
                  - paid
                  - shipped
                  - cancelled
                  - refunded
```

### Modèles incrémentaux

Les tables volumineuses utilisent des modèles dbt incrémentaux. Le modèle ne retraite pas systématiquement les trois années d’historique ; il traite uniquement les nouvelles données et les données récemment modifiées.

Exemple :

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='order_id',
        partition_by={
            'field': 'updated_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        },
        cluster_by=['customer_id']
    )
}}

select
    order_id,
    customer_id,
    order_status,
    amount,
    created_at,
    updated_at,
    _logical_date,
    _ingested_at
from {{ ref('stg_orders') }}

{% if is_incremental() %}
where updated_at >= timestamp_sub(
    current_timestamp(),
    interval 3 day
)
{% endif %}
```

La colonne `unique_key` permet à dbt d’identifier les lignes existantes et de les mettre à jour au lieu de les ajouter systématiquement. Cette clé doit être réellement unique et ne doit pas contenir de valeurs nulles, faute de quoi le modèle peut produire des doublons.

La fenêtre de trois jours permet de récupérer les corrections tardives des sources sans retraiter toute la table. Cette valeur sera ajustée si l’analyse du prestataire de paiement montre que les corrections peuvent intervenir plus tard.

### Documentation et lineage

Chaque modèle et chaque colonne importante doit être documenté dans les fichiers YAML dbt.

Exemple :

```yaml
version: 2

models:
  - name: daily_sales
    description: >
      Chiffre d’affaires journalier calculé à partir des commandes
      validées et des paiements confirmés.

    columns:
      - name: sales_date
        description: "Date métier du chiffre d’affaires."

      - name: total_revenue
        description: "Montant total des commandes retenues."

      - name: order_count
        description: "Nombre de commandes retenues."
```

La documentation dbt permet de consulter les modèles, les descriptions, les tests et le graphe de lineage du projet. 

## Orchestration avec Airflow

Airflow orchestre l’exécution de dbt mais ne contient pas la logique principale de transformation.

Le DAG quotidien suit cet ordre :

```text
01. Extraire Postgres
02. Extraire l’API de paiement
03. Charger les logs clickstream
04. Charger les tables bronze
05. Exécuter dbt staging
06. Exécuter les modèles silver
07. Exécuter les tests dbt silver
08. Exécuter les modèles gold
09. Construire les features churn
10. Exécuter les tests dbt gold
11. Publier les résultats pour les dashboards
```

Exemple simplifié :

```python
extract_postgres >> load_bronze
extract_payments >> load_bronze
load_clickstream >> load_bronze

load_bronze >> dbt_staging
dbt_staging >> dbt_silver
dbt_silver >> dbt_test_silver
dbt_test_silver >> dbt_gold
dbt_gold >> dbt_test_gold
```

Les tests bloquants doivent empêcher la publication des tables gold lorsque les données critiques sont invalides.

## Commandes dbt utilisées

```bash
dbt debug
dbt deps
dbt seed
dbt run
dbt test
dbt build
dbt docs generate
dbt docs serve
```

La commande `dbt build` permet de construire les ressources du projet et d’exécuter les tests dans l’ordre des dépendances. Elle peut notamment exécuter les seeds, snapshots, modèles, tests unitaires et tests de données selon les ressources sélectionnées.

La commande principale de la pipeline sera :

```bash
dbt build --target prod
```

Pour une exécution ciblée :

```bash
dbt build --select staging
dbt build --select silver
dbt build --select gold
dbt build --select +churn_features_daily
```

## Alternatives écartées

- **SQL dispersé dans Airflow** : écarté car les dépendances, la documentation et les tests seraient moins visibles.
- **Scripts Python pour toutes les transformations** : écarté car les transformations sont principalement relationnelles et SQL ; Python ajouterait de la complexité et de la maintenance.
- **Spark pour toutes les transformations** : écarté pour la tranche actuelle car il nécessiterait davantage d’infrastructure et d’exploitation.
- **Procédures SQL directement créées dans BigQuery** : écartées comme mécanisme principal car elles seraient moins faciles à versionner, tester et documenter dans une structure de modèles.
- **Transformation dans l’outil BI** : écartée car les règles métier seraient dispersées et difficiles à réutiliser pour la table de features churn.
- **Full refresh quotidien** : écarté car il serait coûteux et inutile avec 3 ans d’historique.
- **dbt uniquement sans tests bloquants** : écarté car les tables invalides pourraient atteindre les dashboards et le modèle de churn.

## Conséquences

Les transformations sont centralisées, versionnées et organisées par dépendances. Les modèles dbt peuvent être relus, testés et exécutés localement avant leur intégration dans la pipeline.

Les tests dbt réduisent le risque de publier des données incorrectes dans les dashboards ou dans la table de features churn. La documentation et le lineage facilitent également l’analyse de l’impact lorsqu’une source ou une colonne change.

Les modèles incrémentaux réduisent le volume retraité quotidiennement et améliorent le temps d’exécution de la pipeline. En revanche, ils exigent des clés métier fiables, des filtres incrémentaux corrects et une stratégie explicite pour les corrections tardives.

L’équipe doit apprendre la structure d’un projet dbt, Jinja, les modèles incrémentaux, les tests et les conventions de nommage.

Le projet devient dépendant de dbt et de BigQuery, mais ce couplage est accepté car il simplifie l’architecture et correspond au besoin analytique de la plateforme.

## Structure dbt retenue

```text
dbt/
├── dbt_project.yml
├── packages.yml
├── models/
│   ├── staging/
│   │   ├── sources.yml
│   │   ├── stg_orders.sql
│   │   ├── stg_payments.sql
│   │   ├── stg_clickstream.sql
│   │   └── staging.yml
│   ├── silver/
│   │   ├── fct_orders.sql
│   │   ├── fct_payments.sql
│   │   ├── dim_customers.sql
│   │   └── silver.yml
│   └── gold/
│       ├── daily_sales.sql
│       ├── customer_activity.sql
│       ├── churn_features_daily.sql
│       └── gold.yml
├── snapshots/
├── tests/
├── macros/
└── seeds/
```

## Règle de responsabilité

```text
Airflow :
orchestrer les étapes et gérer les dépendances externes

dbt :
transformer, tester, documenter et exposer le lineage

BigQuery :
stocker, partitionner, requêter et agréger les données

BI :
consommer les tables gold

Machine learning :
consommer la table churn_features_daily
```
