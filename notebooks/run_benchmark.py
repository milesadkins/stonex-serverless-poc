# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-H Benchmark: Serverless vs Classic Compute
# MAGIC
# MAGIC This notebook runs all 22 TPC-H benchmark queries against both a **serverless** and
# MAGIC **classic** SQL warehouse, then visualizes the comparison.
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Create (or identify) a **Serverless** SQL warehouse
# MAGIC 2. Create (or identify) a **Classic** SQL warehouse of equivalent size
# MAGIC 3. Fill in the warehouse IDs in the config cell below

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Configuration - Update warehouse IDs
WAREHOUSES = [
    {
        "id": "<SERVERLESS_WAREHOUSE_ID>",
        "name": "Serverless Medium",
        "type": "serverless",
    },
    {
        "id": "<CLASSIC_WAREHOUSE_ID>",
        "name": "Classic Medium",
        "type": "classic",
    },
]

# Number of iterations per query (higher = more stable averages)
ITERATIONS = 1

# Set to None for all 22 queries, or provide a list like ["Q01", "Q06", "Q14"]
QUERY_SUBSET = None

# COMMAND ----------

# DBTITLE 1,Run Benchmark
from databricks.sdk import WorkspaceClient

from tpch_benchmark.runner import run_benchmark, save_results

client = WorkspaceClient()

run = run_benchmark(
    client=client,
    warehouses=WAREHOUSES,
    query_ids=QUERY_SUBSET,
    iterations=ITERATIONS,
)

save_results(run, "/tmp/benchmark_results.json")

# COMMAND ----------

# DBTITLE 1,Results Analysis
import pandas as pd

df = pd.DataFrame([r.__dict__ if hasattr(r, '__dict__') else r for r in run.results])
df_success = df[df["status"] == "SUCCESS"]

# Pivot: query on rows, warehouse type on columns
pivot = df_success.pivot_table(
    index=["query_id", "query_name"],
    columns="warehouse_type",
    values="execution_time_seconds",
    aggfunc="mean",
).reset_index()

if "serverless" in pivot.columns and "classic" in pivot.columns:
    pivot["speedup"] = pivot["classic"] / pivot["serverless"]
    pivot["pct_faster"] = ((pivot["classic"] - pivot["serverless"]) / pivot["classic"] * 100).round(1)

display(pivot.sort_values("query_id"))

# COMMAND ----------

# DBTITLE 1,Visualization
import matplotlib.pyplot as plt

if "serverless" in pivot.columns and "classic" in pivot.columns:
    fig, ax = plt.subplots(figsize=(16, 8))

    x = range(len(pivot))
    width = 0.35

    bars1 = ax.bar([i - width/2 for i in x], pivot["serverless"], width, label="Serverless", color="#FF6F00")
    bars2 = ax.bar([i + width/2 for i in x], pivot["classic"], width, label="Classic", color="#1565C0")

    ax.set_xlabel("TPC-H Query")
    ax.set_ylabel("Execution Time (seconds)")
    ax.set_title("TPC-H Benchmark: Serverless vs Classic SQL Warehouse")
    ax.set_xticks(list(x))
    ax.set_xticklabels(pivot["query_id"], rotation=45)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig("/tmp/benchmark_chart.png", dpi=150)
    plt.show()
else:
    print("Need both serverless and classic results to generate comparison chart.")

# COMMAND ----------

# DBTITLE 1,Summary Statistics
if "serverless" in pivot.columns and "classic" in pivot.columns:
    print(f"Total Serverless time:  {pivot['serverless'].sum():.1f}s")
    print(f"Total Classic time:     {pivot['classic'].sum():.1f}s")
    print(f"Average speedup:        {pivot['speedup'].mean():.2f}x")
    print(f"Median speedup:         {pivot['speedup'].median():.2f}x")
    print(f"\nQueries where Serverless is faster: {(pivot['speedup'] > 1).sum()}/{len(pivot)}")
    print(f"Queries where Classic is faster:    {(pivot['speedup'] < 1).sum()}/{len(pivot)}")
