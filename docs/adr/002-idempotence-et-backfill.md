# ADR 002 — Idempotence et backfill

## Contexte

La plateforme reçoit des données provenant de trois sources avec des comportements différents :

- Postgres contient les commandes et environ 20 Go de changements quotidiens.
- L’API REST du prestataire de paiement impose des quotas et peut corriger des transactions après leur première récupération.
- Les logs JSON de clickstream arrivent quotidiennement et leur schéma peut évoluer.

Le pipeline doit être exécuté chaque nuit afin de rendre les dashboards et la table de features churn disponibles avant 7h. Il doit également être possible de rejouer une journée ou une plage de dates sans créer de doublons ni dégrader les données déjà chargées.

Les données de paiement peuvent changer a posteriori. Une simple stratégie consistant à ajouter uniquement les nouvelles lignes ne suffit donc pas. Le pipeline doit pouvoir réimporter une fenêtre historique et remplacer les versions précédentes des lignes concernées.

Airflow utilise une date logique qui représente le début de l’intervalle de données traité, et non nécessairement l’heure réelle de démarrage du traitement. Cette distinction est importante pour rejouer un traitement avec la même période métier. [web:688][web:689]

## Décision

Le pipeline adopte une stratégie basée sur :

1. une **date logique de traitement** ;
2. une **zone de staging** temporaire ou partitionnée ;
3. une **clé métier stable** ;
4. une intégration par **MERGE** ;
5. des modèles dbt incrémentaux pour les tables volumineuses.

### Date logique

Chaque exécution possède une date logique et une fenêtre de données explicite :

```text
logical_date = 2026-10-20
window_start = 2026-10-20 00:00:00
window_end   = 2026-10-21 00:00:00
```

La date logique correspond à la journée métier traitée. Elle ne doit pas être remplacée par l’heure réelle à laquelle le DAG commence.

Pour un traitement quotidien :

```text
Le DAG lancé le 21 octobre traite les données du 20 octobre.
```

Pour un backfill :

```text
Le DAG peut être relancé avec logical_date = 2026-10-20
afin de reconstruire uniquement la journée du 20 octobre.
```

### Staging

Chaque source est d’abord chargée dans une table de staging avec des métadonnées techniques :

```text
_ingested_at
_logical_date
_source_name
_source_file
_batch_id
_record_hash
```

Exemple :

```text
bronze.orders_staging
bronze.payments_staging
bronze.clickstream_staging
```

La staging permet de conserver la trace du lot reçu avant de modifier les tables silver et gold.

### Clés métier

Les tables utilisent une clé métier stable :

- commandes : `order_id` ;
- paiements : `payment_id` ou identifiant de transaction du prestataire ;
- événements clickstream : `event_id`, ou une clé composée si la source ne fournit pas d’identifiant ;
- features churn : `customer_id` + `feature_date`.

Lorsqu’une source ne fournit pas de clé stable, une clé technique doit être construite à partir de plusieurs attributs déterministes, par exemple :

```text
user_id + event_timestamp + event_name + session_id
```

La clé doit être identique lorsqu’un même enregistrement est retraité.

### MERGE

Les données de staging sont intégrées dans les tables silver avec une opération d’upsert :

```sql
MERGE `project.silver.fct_payments` AS target
USING `project.bronze.payments_staging` AS source
ON target.payment_id = source.payment_id

WHEN MATCHED THEN
  UPDATE SET
    order_id = source.order_id,
    customer_id = source.customer_id,
    payment_status = source.payment_status,
    amount = source.amount,
    updated_at = source.updated_at,
    _ingested_at = source._ingested_at,
    _logical_date = source._logical_date

WHEN NOT MATCHED THEN
  INSERT (
    payment_id,
    order_id,
    customer_id,
    payment_status,
    amount,
    updated_at,
    _ingested_at,
    _logical_date
  )
  VALUES (
    source.payment_id,
    source.order_id,
    source.customer_id,
    source.payment_status,
    source.amount,
    source.updated_at,
    source._ingested_at,
    source._logical_date
  );
```

L’opération `MERGE` met à jour les lignes existantes et insère les nouvelles lignes selon la clé de correspondance. Cette logique permet d’éviter les doublons lors d’un rerun et de prendre en compte les corrections tardives de l’API de paiement. [web:687]

### dbt incrémental

Les tables silver et gold volumineuses sont implémentées avec des modèles dbt incrémentaux :

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='payment_id',
        partition_by={
            'field': 'updated_at',
            'data_type': 'timestamp',
            'granularity': 'day'
        }
    )
}}

select
    payment_id,
    order_id,
    customer_id,
    payment_status,
    amount,
    updated_at,
    _logical_date
from {{ ref('stg_payments') }}

{% if is_incremental() %}
where updated_at >= timestamp_sub(
    current_timestamp(),
    interval 3 day
)
{% endif %}
```

La fenêtre de trois jours permet de récupérer les corrections tardives sans retraiter systématiquement les trois années complètes. Elle doit être ajustée si le prestataire garantit une fenêtre de correction plus longue.

dbt utilise une clé unique avec la stratégie `merge` pour mettre à jour les lignes correspondantes et insérer les nouvelles lignes. Les modèles incrémentaux ne retraitent donc que les données filtrées par la logique du modèle. [web:668][web:669]

## Stratégie de backfill

Un backfill est exécuté avec une date logique ou une plage de dates explicite :

```bash
airflow dags backfill \
  -s 2026-10-01 \
  -e 2026-10-07 \
  ecommerce_daily_pipeline
```

Le backfill suit le même chemin que le traitement normal :

```text
date logique
    ↓
extraction de la fenêtre concernée
    ↓
staging
    ↓
bronze
    ↓
dbt staging
    ↓
dbt silver avec MERGE
    ↓
dbt gold
    ↓
features churn
    ↓
tests et publication
```

Un backfill ne doit pas modifier les données en dehors de la période demandée, sauf lorsqu’une dépendance métier l’exige explicitement.

Pour les tables gold agrégées par jour, les partitions concernées peuvent être recalculées. Pour les modèles dbt incrémentaux basés sur une clé métier, les lignes sont mises à jour avec `MERGE`.

## Alternatives écartées

- **INSERT uniquement** : écarté car le rejeu d’une même journée créerait des doublons.
- **Append avec déduplication uniquement en lecture** : écarté car les doublons resteraient stockés et compliqueraient les dashboards et les features churn.
- **DELETE puis INSERT de toute la table cible** : écarté car le coût et le temps de traitement seraient trop élevés avec 3 ans d’historique.
- **Full refresh systématique des modèles dbt** : écarté car il retraiterait inutilement de gros volumes lors d’une exécution quotidienne.
- **Utiliser l’heure réelle de lancement comme date de traitement** : écarté car un retard ou un rerun produirait une fenêtre incohérente.
- **Ne récupérer que les nouvelles données de l’API de paiement** : écarté car les transactions peuvent être corrigées après leur première ingestion.
- **Stocker uniquement la dernière version des paiements sans métadonnées** : écarté car cela réduirait la capacité d’audit et de diagnostic.

## Conséquences

La stratégie rend les reruns idempotents : exécuter deux fois le même traitement avec la même date logique produit le même état final dans les tables silver et gold.

Les corrections tardives du prestataire de paiement peuvent être intégrées en réouvrant une fenêtre historique et en appliquant un `MERGE`.

Les backfills peuvent être exécutés sur une journée ou une plage de dates sans reconstruire systématiquement les trois années d’historique.

Le pipeline doit toutefois respecter plusieurs contraintes :

- chaque table doit avoir une clé métier fiable ;
- les données de staging doivent être conservées assez longtemps pour permettre le diagnostic ;
- les modèles dbt doivent définir correctement `unique_key` et le filtre incrémental ;
- les tables doivent être partitionnées et éventuellement clusterisées pour limiter les coûts ;
- les tests dbt doivent détecter les doublons avant la publication des tables gold ;
- les fenêtres de reprise doivent être documentées par source.

La stratégie `MERGE` privilégie un modèle de type SCD de type 1 : la version corrigée remplace la version précédente. Si l’historique complet des changements de paiement devient nécessaire, une table d’historisation séparée devra être ajoutée.