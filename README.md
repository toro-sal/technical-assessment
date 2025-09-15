# 🧹 Part 0 — Data Cleaning & Validation

Before loading any data into BigQuery or running dbt models, the raw CSVs are cleaned and validated using a pandas-based script.  

This ensures the raw data complies with the business rules and referential integrity constraints.

## Steps

- **File validation**
  - Ensure `data/affiliates.csv`, `data/players.csv`, `data/transactions.csv` exist

- **Column detection & type coercion**
  - Detect key columns dynamically
  - Convert string booleans to `True/False`
  - Parse dates to UTC-safe `datetime`

- **Business rules enforcement**
  - ✅ Remove duplicate IDs (`ensure_unique_ids`)
  - ✅ Ensure affiliate codes are unique and valid
  - ✅ Remove affiliates pointing to non-existent players
  - ✅ Remove transactions from non-KYC players
  - ✅ Remove transactions before the player’s `kyc_verified_at`
  - ✅ Remove orphan transactions (player_id not found)

- **Output**
  - Clean CSVs saved in `data/cleaned/`:
    ```
    affiliates_clean.csv  
    players_clean.csv  
    transactions_clean.csv
    ```

---

# Extended Data Generation (1000 rows each)

Once the clean datasets are available, a second script automatically extends them to ensure each table has **at least 1000 rows**, which is useful for realistic testing of the dbt models.

## Logic

- **Detects `data/cleaned` and writes to `data/extended_1000/`**
- **Affiliates**
  - Generate unique `affiliate_id` and `affiliate_code`
  - Random assign `owner_player_id` from existing players
  - Fill missing `origin` from a fixed pool (web, ios, Discord, etc.)
  - Estimate `redeemed_at` from player signup / KYC date + random jitter

- **Players**
  - Generate `player_id`, `created_at`, `updated_at`, `country_code`
  - Assign 60% of players a valid `affiliate_code_redeemed`
  - Ensure `kyc_verified` ≈ 75% True
  - If `kyc_verified=True`, ensure `kyc_verified_at` ≥ `created_at`
  - Map `affiliate_id` from redeemed codes

- **Transactions**
  - Generate `transaction_id`, `player_id`, `timestamp`, `amount`
  - Only for KYC-approved players
  - Random `type` among `deposit`, `withdrawal`, `bet`, `win`
  - Ensure `txn_date` ≥ player `kyc_verified_at`

- **Validation rules**
  - No transactions before KYC
  - `affiliate_id` ↔ `affiliate_code` consistent
  - Unique IDs
  - At least 1000 rows each

- **Outputs**
data/extended_1000/affiliates_1000.csv
data/extended_1000/players_1000.csv
data/extended_1000/transactions_1000.csv





---

# 🧠 Part 1 — Data Model (dbt) — Design Rationale

## Goals & inputs

- Build a trustworthy analytics layer from three raw domains:
- `players` (KYC status & metadata)
- `transactions` (deposits/withdrawals)
- `affiliates` (codes, origin, redemptions)
- Enforce business rules (KYC before transactions, 1:1 affiliate redemption mapping, linkage integrity)
- Provide analytics outputs requested:
- **Model 1**: one row per player per day with deposits & withdrawals (withdrawals negative)
- **Model 2**: deposit sum & count per player country for **KYC-approved** players with **Discord** affiliate origin
- **Model 3**: top-3 deposit amounts per player

---

## Architecture: Medallion (bronze → silver → gold)

- **Bronze**: typed/normalized copies of raw data (no business logic)  
- **Silver**: cleaned, deduplicated, and **business rules enforced**  
- **Gold**: analytics-ready fact/aggregate tables for the explicit requirements

---

## Conventions & standards

- One model per file
- Lowercase folder names aligned with `dbt_project.yml`
- Explicit types with `cast()`, macros for safe parsing
- Avoid reserved words like `timestamp`
- Partitioning on timestamp/date fields for performance
- Naming: `br_*`, `sl_*`, `g_*`

---

## Bronze models

**`br_players`**  
Normalize & type `players` source  
- Parse timestamps, normalize booleans, cast ids

**`br_transactions`**  
Type-safe copy of transactions  
- Parse `timestamp` → `txn_timestamp`

**`br_affiliates`**  
Clean and type affiliate codes and timestamps

---

## Silver models

**`sl_players`**  
- Deduplicate, compute `kyc_approved_at`  
- Ensure one row per player

**`sl_affiliates`**  
- Deduplicate affiliates

**`sl_affiliate_player_bridge`**  
- Ensure affiliate ↔ player linkage integrity  
- Flags 1:1 violations and unlinked redemptions

**`sl_transactions`**  
- Filter out transactions before `kyc_approved_at`  
- Join to `sl_players` for gating  
- Partition by `txn_timestamp`

---

## Gold models

**`g_player_daily_balance`**  
- Daily deposits and withdrawals (withdrawals negative)

**`g_country_deposits_discord`**  
- Deposits by country for KYC-approved players from Discord

**`g_top3_player_deposits`**  
- Top 3 deposits per player by amount

---

## Business rule → enforcement map

| Business Rule                                  | Implemented In                |
|------------------------------------------------|--------------------------------|
| 1. No transactions before KYC                   | `sl_transactions`               |
| 2. 1:1 affiliate redemption                     | `sl_affiliate_player_bridge`   |
| 3. Affiliate redeemed must link to player       | `sl_affiliate_player_bridge`   |
| 4. Players may join without affiliate            | Left joins allowed              |
| 5. IDs unique                                   | dbt schema tests                |

---

## Macros & tests

- `normalize_country_iso2`, `parse_bool_safe`, `parse_timestamp_safe`, `parse_numeric_safe`, `deduplicate`
- Tests:
- uniqueness, not null, accepted values, referential integrity, data quality assertions

---

## Snapshot

- `snapshots/players_snapshot.sql`  
- Strategy: timestamp on `updated_at`  
- Tracks KYC state history

---

# ⚙️ Part 2 — Pipeline (Airflow, GCS, BigQuery, dbt)

**Flow**  
1. CSV → Parquet  
2. Parquet → GCS  
3. GCS → BigQuery (bronze)  
4. dbt bronze → snapshot → silver → gold

**Ordering**  
- `sl_transactions` runs last inside silver group because it depends on `sl_players`

**Tests**  
- Run after bronze/silver/gold groups with `allow_fail=True` (soft fail)

**Performance**  
- Partitioned models reduce scan cost

**Security**  
- GCP credentials from Airflow Variables as `GOOGLE_APPLICATION_CREDENTIALS`

**Troubleshooting**  
- Align folder names with `dbt_project.yml` keys  
- Never use `timestamp` as column alias  
- Use `+model` to include parents when running isolated models

## Cloud Resources

**BigQuery (project · dataset):**  
- Project ID: `brilliant-dryad-434600-f5`  
- Dataset: `ancient`  
- Fully-qualified: `brilliant-dryad-434600-f5.ancient`  
- Console: https://console.cloud.google.com/bigquery?ws=!1m4!1m3!3m2!1sbrilliant-dryad-434600-f5!2sancient

**GCS Bucket (raw + parquet landing):**  
- Bucket: `ancientg_tecnical`  
- Console: https://console.cloud.google.com/storage/browser/ancientg_tecnical


**GCP Service acount**  

Put the service account  in the followin path  technical-assessment/gcp
