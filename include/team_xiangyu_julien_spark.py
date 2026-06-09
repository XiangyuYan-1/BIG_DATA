from __future__ import annotations

from pathlib import Path
import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper, when, sum, count


def transform_1(spark: SparkSession, logical_date: str) -> DataFrame:

    path = f"/opt/airflow/data/raw/dt={logical_date}"

    df = spark.read.parquet(path)

    df = df.filter(col("amount_eur") > 0)

    return df


def transform_2(spark: SparkSession, df: DataFrame, logical_date: str) -> DataFrame:

    df = df.withColumn(
        "country_upper",
        upper(col("country"))
    )

    df = df.withColumn(
        "amount_bucket",
        when(col("amount_eur") >= 100, "high")
        .when(col("amount_eur") >= 50, "medium")
        .otherwise("low")
    )

    return df


def transform_3(df: DataFrame) -> DataFrame:

    result = (
        df.groupBy(
            "country_upper",
            "category"
        )
        .agg(
            sum("amount_eur").alias("revenue"),
            count("*").alias("transaction_count")
        )
    )

    return result


def run_daily(logical_date: str,
              with_reference: bool = False) -> dict:

    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("team_xiangyu_julien")
        .getOrCreate()
    )

    df1 = transform_1(
        spark,
        logical_date
    )

    df2 = transform_2(
        spark,
        df1,
        logical_date
    )

    final_df = transform_3(df2)

    curated_path = (
        f"/opt/airflow/data/curated/dt={logical_date}"
    )

    final_df.write.mode(
        "overwrite"
    ).parquet(curated_path)

    total_revenue = final_df.agg(
        sum("revenue")
    ).collect()[0][0]

    total_transactions = final_df.agg(
        sum("transaction_count")
    ).collect()[0][0]

    total_kpi_rows = final_df.count()

    report = {

        "status": "success",
        "logical_date": logical_date,
        "total_revenue": float(total_revenue),
        "total_transactions": int(total_transactions),
        "total_kpi_rows": int(total_kpi_rows),
        "curated_path": curated_path,
        "idempotence": "outputs are overwritten for the same logical date"

    }

    report_path = Path(
        f"/opt/airflow/data/reports/dashboard_{logical_date}.json"
    )

    report_path.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8"
    )

    spark.stop()

    return report