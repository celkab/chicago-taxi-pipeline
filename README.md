# Chicago Taxi Trips — pipeline Bronze → Silver → Gold (Dagster + PySpark + Docker)

## 1. Prérequis et lancement

**Prérequis** : Docker Desktop (WSL2 sous Windows) ou Docker Engine + Compose v2, port **3000** libre,
4 à 8 Go de RAM alloués à Docker, accès Internet (API SODA + build de l'image).

```bash
docker compose up --build
```

- Le service `pipeline` démarre l'interface Dagster : **http://localhost:3000**.
- Le service `pipeline-run` exécute automatiquement `dagster asset materialize --select '*'` (bronze → silver → gold) puis s'arrête.
  Les données apparaissent dans `./data/` (`bronze`, `silver`, `rejected`, `gold`).
- Alternative manuelle : dans l'UI, onglet **Assets** → **Materialize all**.
- Pour tester à zéro : `docker compose down -v && rm -rf data && docker compose up --build`.

## 2. Données

- Dataset : *Chicago Taxi Trips (2013-2023)*, id SODA `wrvz-psew` (le jeu s'arrête fin 2023).
- **Période choisie : du 2023-01-01 au 2023-01-08 (exclu), soit 7 jours.** Elle se change sans toucher au code :
  `START_DATE=2023-01-01 END_DATE=2023-04-01 docker compose up --build` (un trimestre).
- **Volume obtenu** : le nombre de lignes de chaque jour est loggé par l'asset `bronze_trips` (onglet Runs de Dagster)
  et le total silver/rejeté par `silver_trips`. *(À reporter ici après le premier run : ___ lignes.)*
- **Téléchargement** (fait par l'asset `bronze_trips`) : une requête par jour, paginée par tranches de 100 000 lignes. Équivalent `curl` pour une page :

```bash
curl -G "https://data.cityofchicago.org/resource/wrvz-psew.csv" \
  --data-urlencode "\$where=trip_start_timestamp >= '2023-01-01T00:00:00' AND trip_start_timestamp < '2023-01-02T00:00:00'" \
  --data-urlencode "\$order=:id" --data-urlencode "\$limit=50000" --data-urlencode "\$offset=0"
```

Le téléchargement est **idempotent** (un jour déjà présent n'est pas retéléchargé) et **réessaie** 3 fois en cas d'erreur réseau.

## 3. Architecture

```
API SODA (CSV) ──► BRONZE ──────────► SILVER ────────────► GOLD
 pagination        data/bronze/       data/silver/          data/gold/
 par jour          trips_<jour>.csv   Parquet partitionné   revenue_per_day
                   (brut, inchangé)   par trip_date         avg_duration_per_day
                                      │                     top_pickup_zones
                                      └─► data/rejected/ (lignes écartées + raison)

        Orchestration : Dagster (assets)     Traitement : PySpark local[*]     Exécution : Docker Compose
```

Assets Dagster : `bronze_trips → silver_trips → gold_revenue_per_day | gold_avg_duration_per_day | gold_top_pickup_zones`.
Tous les chemins sont dans `pipeline/config.py` : passer plus tard à MinIO (`s3a://…`) ne demande de changer que ce fichier.

## 4. Choix techniques

- **Dagster** : le modèle « asset » correspond directement aux couches du médaillon, avec lineage visible dans l'UI.
- **Spark en mode local** : suffisant pour ce volume, aucun cluster à gérer.
- **Silver : lecture en string puis cast explicite** : on contrôle le typage nous-mêmes. Spark 4 active ANSI par défaut, ce qui ferait
  planter le job sur une valeur illisible ; on le désactive (`spark.sql.ansi.enabled=false`) pour obtenir un `NULL`, que les règles de qualité rejettent proprement.
- **Déduplication sur `trip_id`** : c'est l'identifiant unique d'une course ; un même trajet rechargé deux fois ne doit compter qu'une fois dans le CA.
- **Parquet partitionné par `trip_date`** : format colonnaire typé et compressé, et les agrégats « par jour » ne lisent que les partitions utiles.
- **Quarantaine plutôt que suppression** : les lignes invalides ne sont pas perdues, elles sont écrites dans `data/rejected/` avec la colonne `reject_reasons`,
  et le nombre de rejets par règle est loggé.

### Règles de qualité (silver)

| Règle | Une ligne est rejetée si… | Pourquoi |
|---|---|---|
| `trip_id_null` | `trip_id` est nul | clé de déduplication, traçabilité |
| `start_null` | le début est nul ou illisible | impossible de rattacher la course à un jour (`trip_date`) |
| `duration_not_positive` | la durée est nulle, illisible ou `<= 0` | une course dure forcément quelque chose ; fausserait la durée moyenne |
| `total_negative` | le total est nul, illisible ou `< 0` | le total alimente le chiffre d'affaires |
| `end_before_start` | la fin est avant le début | incohérence temporelle |

Les courses sans zone de prise en charge sont conservées (elles comptent dans le CA) mais exclues du top des zones.

### Tables gold

- `revenue_per_day` : `trip_date, revenue, nb_trips`
- `avg_duration_per_day` : `trip_date, avg_duration_min, nb_trips`
- `top_pickup_zones` : les 10 `pickup_community_area` avec le plus de courses

## 5. Ce que je ferais avec plus de temps

- **Stockage objet** : MinIO / S3 (`s3a://`) comme socle du datalake, avec le JAR `hadoop-aws` ; ici le datalake est un volume local.
- **Qualité plus poussée** : seuils sur les valeurs aberrantes (durée max, distance max, total max), montants négatifs sur `fare/tips/tolls/extras`,
  rapport qualité persisté, contrôles Dagster (*asset checks*) avec seuil d'alerte sur le taux de rejet, réconciliation gold/silver.
- **Ingestion** : téléchargement parallèle, écriture atomique des fichiers, partitions journalières Dagster pour ne retraiter que les jours modifiés.
- **Gold** : jointure avec un référentiel des noms de zones, demande par heure, revenus par type de paiement ou compagnie.
- **Couche d'exposition** : base SQL (DuckDB / Trino) ou dashboard (Superset, Metabase) devant les tables gold.
- **Performance Spark** : moins de partitions de shuffle en local, un fichier Parquet par jour (`repartition`), écrasement dynamique des partitions.
- **Tests et industrialisation** : tests unitaires des transformations (pytest), CI, Delta Lake, planification quotidienne (schedule Dagster),
  séparation webserver / daemon, Spark sur cluster.
