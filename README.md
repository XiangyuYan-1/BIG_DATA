# Team: <Xiangyu Yan> & <Julien Blondiaux>

**DAG id:** `team_xiangyu_julien`  
**Git repo:** `https://github.com/XiangyuYan-1/BIG_DATA` - **also on your Moodle slides** (title or architecture)  
**Spark module:** `include/team_<shortname>_spark.py`  
**Course:** Big Data Processing - Lab 4 Capstone

---

## 1. Business problem

Operations teams need a daily KPI dashboard to monitor transaction revenue and transaction counts by country and category.

If the pipeline fails, new transaction files are not processed, KPI metrics become outdated, and business users cannot monitor daily activity.

**Defense tip:** for each section below, be ready to say **what you built** and **why** (not only that it runs).

**Submit by June 9, 23:59:** push capstone to **your pair's Git repo**; upload **slides on Moodle** with the **same URL** visible on the slides (title slide). Public repo, or private with instructor read access.

---

## 2. Architecture

<!-- Diagram: incoming → raw/dt= → curated/dt= → reports -->

| Layer | Path | Tool |
|-------|------|------|
| Bronze | `data/incoming/` | `vendor_drop.py` |
| Silver | `data/raw/dt=` | DuckDB (`ingest_day`) |
| Gold | `data/curated/dt=` | **Your** `team_yan_spark.py` |
| Serve | `data/reports/` | JSON dashboard |

### Airflow (5 tasks)

| task_id | Role |
|---------|------|
| `wait_csv` | Wait for vendor transaction CSV |
| `ingest_task` | Convert CSV data into raw parquet format |
| `validate_task` | Validate generated silver dataset |
| `spark_task` | Execute Spark KPI pipeline |
| `report_task` | Generate dashboard JSON |
| `notify_task` | Notify pipeline completion |

**Dependency graph:**

```
wait_csv → ingest_task → validate_task → spark_task → report_task → notify_task
```

---

## 3. Spark transformations (≥3 - your code)

File: `include/team_yan_spark.py`

| # | Function | What it does |
|---|----------|--------------|
| 1 | `transform_1` | Read parquet data and filter invalid transactions |
| 2 | `transform_2` | Enrich dataset with additional derived columns |
| 3 | `transform_3` | Aggregate KPI metrics by country and category |

---

## 4. Idempotence

Re-running the same `ds` overwrites existing outputs using overwrite mode.

`raw/dt=<ds>` is overwritten during ingestion.

`curated/dt=<ds>` is overwritten during Spark output.

`dashboard_<ds>.json` is regenerated and overwritten.

---

## 5. Backfill

docker compose exec airflow-scheduler \
  airflow dags backfill team_yan -s 2026-06-01 -e 2026-06-07 --reset-dagruns


## 6. Failure demo

python scripts/vendor_drop.py --date 2026-06-03 --corrupt

<Which task fails? What appears in the Airflow UI?>

The validate_task or spark_task is expected to fail depending on the corrupted records.
In Airflow UI, the failed task becomes red and downstream tasks remain blocked or skipped.

## 7. Exploration tracks

| Track | Done? | Describe your implementation |
|-------|-------|----------|
| R Reliability | Yes | Retries are configured in the DAG with `retries=2` and `retry_delay=3 minutes`. |
| S Spark depth | Yes | Three Spark transformations are implemented: read/filter, enrich, and aggregate. |
| O Orchestration | Yes | Six Airflow tasks are connected in a clear dependency graph. |
| Q Data quality | Yes | A validation task checks the silver dataset before Spark processing. |
| P Custom | No | Not implemented. |
| X SparkSubmit | No | Not implemented. |

---

## 8. Demo script & backup

1. Run the vendor simulator to generate the daily CSV file.
2. Trigger the `team_yan` DAG in Airflow.
3. Show the Airflow graph with all tasks in green.
4. Check `data/raw/dt=<ds>` for the raw parquet output.
5. Check `data/curated/dt=<ds>` for the KPI parquet output.
6. Open `data/reports/dashboard_<ds>.json` to show the dashboard result.
7. If the live demo fails, use backup screenshots of the green DAG run and generated outputs.

---

## 9. Production next steps

In production, the pipeline should include stronger monitoring, alerting, and logging. The Spark job could be moved from local mode to a real Spark cluster. The dashboard JSON could also be stored in a database or connected to a BI tool. More data quality checks and automated tests should be added before deployment.
