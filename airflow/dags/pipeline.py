from __future__ import annotations
import os, glob, logging
from datetime import datetime, timedelta

import pendulum
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from google.cloud import storage, bigquery

# ─────────────────────────────
# Base config
# ─────────────────────────────
LOCAL_TZ = pendulum.timezone("America/Bogota")
DEFAULT_ARGS = {
    "start_date": datetime(2025, 9, 1, tzinfo=LOCAL_TZ),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}
DAG_ID = "technical_assement_pipeline"  # ← renamed
SCHEDULE = None  # manual; set a cron if needed

# dbt paths (match your docker-compose mounts)
DBT_PROJECT_DIR = "/opt/dbt"
DBT_PROFILES_DIR = "/opt/dbt"

def _ensure_gcp_creds() -> None:
    cred = Variable.get("GOOGLE_CREDENTIALS_PATH")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred

def _list_local_csvs(base_dir: str) -> list[str]:
    return sorted(glob.glob(os.path.join(base_dir, "*.csv")))

def _to_parquet(csv_path: str) -> str:
    df = pd.read_csv(csv_path, low_memory=False)
    df = df.loc[:, ~df.columns.astype(str).str.contains(r"^Unnamed", case=False, regex=True)]
    parquet_path = os.path.splitext(csv_path)[0] + ".parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path

# ─────────────────────────────
# dbt helpers
# ─────────────────────────────
def _render_dbt_command(
    resource_type: str = "run",
    select: str | None = None,
    profiles_dir: str = DBT_PROFILES_DIR,
    project_dir: str = DBT_PROJECT_DIR,
    dbt_target: str | None = None,
) -> str:
    sub = {
        "debug": "debug",
        "deps": "deps",
        "seed": "seed",
        "run": "run",
        "test": "test",
        "build": "build",
        "snapshot": "snapshot",
        "compile": "compile",
        "docs": "docs generate",
        "ls": "ls",
    }.get(resource_type, "run")

    export = f"export DBT_TARGET={dbt_target or 'dev_bq'}"
    chooser = (
        'if [ -x "$HOME/.local/bin/dbt" ]; then '
        'CMD="$HOME/.local/bin/dbt"; '
        'else '
        'CMD="python -m dbt.cli.main"; '
        'fi'
    )

    args = f"{sub} --project-dir {project_dir} --profiles-dir {profiles_dir}"
    if select:
        args += f" --select {select}"

    return f'{export} && {chooser} && $CMD {args}'

def create_dbt_task(
    task_id: str,
    dbt_resource: str = "",
    resource_type: str = "run",
    *,
    dbt_target_var_key: str = "DBT_TARGET",
    allow_fail: bool = False,  # when True, do not fail task if dbt returns non-zero
) -> BashOperator:
    target = Variable.get(dbt_target_var_key, default_var="dev_bq")
    base_cmd = _render_dbt_command(
        resource_type=resource_type,
        select=(dbt_resource or None),
        dbt_target=target,
    )

    if allow_fail:
        bash_command = f"""
set +e
{base_cmd}
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
  echo "[WARN] dbt {resource_type} for '{dbt_resource or 'ALL'}' exited with code $EXIT_CODE."
  echo "[WARN] Marking task SUCCESS to avoid blocking downstream."
fi
exit 0
""".strip()
    else:
        bash_command = base_cmd

    env = {
        "DBT_TARGET": target,
        "GOOGLE_APPLICATION_CREDENTIALS": "/opt/airflow/gcp/key.json",  # ensure mounted
        "GCP_PROJECT_ID": Variable.get("BQ_PROJECT", default_var="brilliant-dryad-434600-f5"),
        "BQ_DATASET": Variable.get("BQ_DATASET", default_var="ancient"),
        "BQ_LOCATION": Variable.get("BQ_LOCATION", default_var="US"),
        "PATH": "/home/airflow/.local/bin:/usr/local/bin:" + os.environ.get("PATH",""),
    }
    return BashOperator(task_id=task_id, bash_command=bash_command, env=env)

# ─────────────────────────────
# Main DAG
# ─────────────────────────────
@dag(
    dag_id=DAG_ID,
    description="Convert CSV→Parquet, upload to GCS, load to BigQuery; then run dbt (bronze→snapshot→silver→gold).",
    schedule=SCHEDULE,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gcs", "parquet", "bigquery", "dbt", "medallion"],
)
def technical_assement_pipilile():  # ← function name matches the DAG
    @task
    def prepare_cfg() -> dict:
        _ensure_gcp_creds()
        return {
            "local_dir":  Variable.get("LOCAL_DATA_DIR", default_var="/opt/airflow/data"),
            "bucket":     Variable.get("GCS_BUCKET_NAME", default_var="ancientg_tecnical"),
            "bq_project": Variable.get("BQ_PROJECT", default_var="brilliant-dryad-434600-f5"),
            "bq_dataset": Variable.get("BQ_DATASET", default_var="ancient"),
        }

    @task
    def convert_to_parquet(cfg: dict) -> list[dict]:
        csvs = _list_local_csvs(cfg["local_dir"])
        if not csvs:
            raise ValueError(f"No CSV files found in {cfg['local_dir']}")
        outputs = []
        for csv in csvs:
            pq = _to_parquet(csv)
            base = os.path.basename(pq)
            name_noext = os.path.splitext(base)[0]
            outputs.append({"parquet": pq, "name": name_noext})
        logging.info("Generated Parquets: %s", outputs)
        return outputs

    @task
    def upload_each_to_gcs(cfg: dict, files: list[dict]) -> list[dict]:
        client = storage.Client()
        bucket = client.bucket(cfg["bucket"])
        out = []
        for f in files:
            local = f["parquet"]
            name = f["name"]
            remote = f"raw_{name}.parquet"
            bucket.blob(remote).upload_from_filename(local)
            uri = f"gs://{cfg['bucket']}/{remote}"
            out.append({"table": name, "uri": uri})
            logging.info("Uploaded %s → %s", local, uri)
        return out

    @task
    def load_each_to_bq(cfg: dict, objs: list[dict]):
        client = bigquery.Client()
        dataset = cfg["bq_dataset"]
        project = cfg["bq_project"]

        for obj in objs:
            table_id = f"{project}.{dataset}.{obj['table']}"
            uri = obj["uri"]
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                autodetect=True,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            )
            client.load_table_from_uri([uri], table_id, job_config=job_config).result()
            rows = client.get_table(table_id).num_rows
            logging.info("Loaded %s rows into %s", rows, table_id)

    # ── ETL orchestration ───────────────────────────────────
    cfg = prepare_cfg()
    converted = convert_to_parquet(cfg)
    uploaded = upload_each_to_gcs(cfg, converted)
    loaded = load_each_to_bq(cfg, uploaded)

    # ── Optional dbt sanity (debug / deps) ──────────────────
    dbt_debug = create_dbt_task("dbt_debug", "", "debug")
    # dbt_deps  = create_dbt_task("dbt_deps",  "", "deps")

    # ── BRONZE models ───────────────────────────────────────
    bronze_models = ["br_affiliates", "br_players", "br_transactions"]

    with TaskGroup(group_id="dbt_bronze") as dbt_bronze:
        start = EmptyOperator(task_id="start")
        done  = EmptyOperator(task_id="done")

        for m in bronze_models:
            t = create_dbt_task(task_id=f"run__{m}", dbt_resource=m, resource_type="run")
            start >> t >> done

    # tests: soft-fail so DAG continues
    dbt_test_bronze = create_dbt_task(
        "dbt_test_bronze", " ".join(bronze_models), "test", allow_fail=True
    )

    # ── SNAPSHOT (after bronze; reads from br_players) ──────
    dbt_snapshot_players = create_dbt_task("dbt_snapshot_players", "", "snapshot")

    # ── SILVER models (clean + business rules) ──────────────
    silver_models = ["sl_affiliates", "sl_players", "sl_affiliate_player_bridge", "sl_transactions"]

    # ── SILVER models (clean + business rules) ──────────────
    with TaskGroup(group_id="dbt_silver") as dbt_silver:
        start = EmptyOperator(task_id="start")
        done  = EmptyOperator(task_id="done")

        # run these three first, in order
        run_sl_affiliates = create_dbt_task(
            task_id="run__sl_affiliates",
            dbt_resource="sl_affiliates",     # parents not needed; this is a base silver model
            resource_type="run",
        )
        run_sl_players = create_dbt_task(
            task_id="run__sl_players",
            dbt_resource="sl_players",
            resource_type="run",
        )
        run_sl_bridge = create_dbt_task(
            task_id="run__sl_affiliate_player_bridge",
            dbt_resource="sl_affiliate_player_bridge",
            resource_type="run",
        )

        # ensure sl_transactions runs LAST (after affiliates, players, and bridge)
        run_sl_transactions = create_dbt_task(
            task_id="run__sl_transactions",
            dbt_resource="+sl_transactions",  # include parents defensively (ok even if already run)
            resource_type="run",
        )

        # ordering inside the TaskGroup
        start >> run_sl_affiliates >> run_sl_players >> run_sl_bridge >> run_sl_transactions >> done

    # tests run after ALL silver models; keep soft-fail if desired
    dbt_test_silver = create_dbt_task(
        "dbt_test_silver",
        "sl_affiliates sl_players sl_affiliate_player_bridge sl_transactions",
        "test",
        allow_fail=True,
    )


    # ── GOLD models (analytics outputs) ─────────────────────
    gold_models = ["g_player_daily_balance", "g_country_deposits_discord", "g_top3_player_deposits"]

    with TaskGroup(group_id="dbt_gold") as dbt_gold:
        start = EmptyOperator(task_id="start")
        done  = EmptyOperator(task_id="done")

        for m in gold_models:
            t = create_dbt_task(task_id=f"run__{m}", dbt_resource=m, resource_type="run")
            start >> t >> done

    dbt_test_gold = create_dbt_task(
        "dbt_test_gold", " ".join(gold_models), "test", allow_fail=True
    )

    # ── Dependencies: ETL → bronze → snapshot → silver → gold ──────────────────────
    loaded  >> dbt_bronze >> dbt_test_bronze >> dbt_snapshot_players >> dbt_silver >> dbt_test_silver >> dbt_gold >> dbt_test_gold
    # If you enable deps: loaded >> dbt_debug >> dbt_deps >> dbt_bronze ...

dag = technical_assement_pipilile()
