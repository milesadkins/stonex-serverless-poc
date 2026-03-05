# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-H Benchmark: Cost Analysis
# MAGIC
# MAGIC Analyzes benchmark results and billing data for a given `benchmark_run_id`.
# MAGIC Run this **after billing data has been ingested** (up to 2-3 hours after benchmark jobs complete).
# MAGIC
# MAGIC See `README.md` for full pricing methodology.

# COMMAND ----------

# DBTITLE 1,Configuration
from pyspark.sql import functions as F

# --- Azure VM Pricing (hourly, pay-as-you-go list prices for your region) ---
# Source: https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/
VM_PRICING = {
    "Standard_D8s_v3": {"on_demand_hr": 0.384, "spot_hr": 0.077},
    "Standard_D8ds_v5": {"on_demand_hr": 0.453, "spot_hr": 0.113},
    "Standard_F8s_v2": {"on_demand_hr": 0.338, "spot_hr": 0.068},
}

# --- Customer Discount Rates ---
DBU_DISCOUNT = 0.39       # 39% discount on Databricks DBUs
AZURE_VM_DISCOUNT = 0.60  # 60% discount on Azure VM instances (e.g. reservations)

# --- Parameters from job (set via databricks.yml variables) ---
dbutils.widgets.text("benchmark_run_id", "")
dbutils.widgets.text("results_catalog", "main")
dbutils.widgets.text("results_schema", "default")
dbutils.widgets.text("node_type_id", "Standard_D8s_v3")
dbutils.widgets.text("num_workers", "2")

benchmark_run_id = dbutils.widgets.get("benchmark_run_id")
RESULTS_TABLE = f"{dbutils.widgets.get('results_catalog')}.{dbutils.widgets.get('results_schema')}.tpch_benchmark_results"
NODE_TYPE = dbutils.widgets.get("node_type_id")
NUM_NODES = int(dbutils.widgets.get("num_workers")) + 1  # workers + 1 driver

assert benchmark_run_id, "benchmark_run_id is required -- pass it as a job parameter or widget value"

print(f"Benchmark run ID: {benchmark_run_id}")
print(f"Results table: {RESULTS_TABLE}")
print(f"Node type: {NODE_TYPE}, Nodes: {NUM_NODES}")

# COMMAND ----------

# DBTITLE 1,Execution Time Pivot (all 4 configs)
pivot_df = spark.sql(f"""
WITH by_type AS (
    SELECT query_id, query_name, compute_type, avg(execution_time_seconds) AS avg_time
    FROM {RESULTS_TABLE} WHERE status = 'SUCCESS' AND benchmark_run_id = '{benchmark_run_id}'
    GROUP BY query_id, query_name, compute_type
)
SELECT
    query_id, query_name,
    round(MAX(CASE WHEN compute_type = 'serverless_perf_optimized' THEN avg_time END), 3) AS serverless_perf,
    round(MAX(CASE WHEN compute_type = 'serverless_cost_optimized' THEN avg_time END), 3) AS serverless_cost,
    round(MAX(CASE WHEN compute_type = 'classic_on_demand' THEN avg_time END), 3) AS classic_on_demand,
    round(MAX(CASE WHEN compute_type = 'classic_spot' THEN avg_time END), 3) AS classic_spot
FROM by_type
GROUP BY query_id, query_name
ORDER BY query_id
""")
display(pivot_df)

# COMMAND ----------

# DBTITLE 1,Cost Calculation Functions

def apply_dbu_discount(list_price: float) -> float:
    """Apply customer DBU discount to list price."""
    return list_price * (1 - DBU_DISCOUNT)


def compute_vm_cost(compute_type: str, execution_seconds: float, node_type: str = NODE_TYPE) -> float:
    """Calculate Azure VM cost for classic compute based on execution time only.

    Serverless has no VM cost (infrastructure is managed by Databricks).
    Classic on-demand: all nodes charged at on-demand rate * (1 - AZURE_VM_DISCOUNT).
    Classic spot: 1 on-demand driver (discounted) + (N-1) spot workers (market rate, no further discount).
    """
    if "serverless" in compute_type:
        return 0.0

    vm_prices = VM_PRICING.get(node_type, VM_PRICING["Standard_D8s_v3"])
    execution_hours = execution_seconds / 3600
    discount_factor = 1 - AZURE_VM_DISCOUNT

    if "spot" in compute_type:
        driver_cost = vm_prices["on_demand_hr"] * discount_factor
        worker_cost = vm_prices["spot_hr"]
        return (1 * driver_cost + (NUM_NODES - 1) * worker_cost) * execution_hours
    else:
        return NUM_NODES * vm_prices["on_demand_hr"] * discount_factor * execution_hours

# COMMAND ----------

# DBTITLE 1,Cost Analysis: Full Cost Summary
workspace_id = spark.conf.get("spark.databricks.workspaceId", "unknown")

# Get execution times from benchmark results
exec_df = spark.sql(f"""
SELECT
    compute_type,
    SUM(execution_time_seconds) AS total_exec_seconds,
    COUNT(*) AS num_queries,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS succeeded
FROM {RESULTS_TABLE}
WHERE benchmark_run_id = '{benchmark_run_id}'
GROUP BY compute_type
""").collect()

# Get actual DBU consumption + list prices from system tables
billing_df = spark.sql(f"""
SELECT
    usage_metadata.job_name AS job_name,
    usage_metadata.job_run_id AS job_run_id,
    u.sku_name,
    product_features.is_serverless,
    product_features.performance_target,
    ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
    ROUND(ANY_VALUE(lp.pricing.effective_list.default), 2) AS price_per_dbu,
    ROUND(SUM(u.usage_quantity) * ANY_VALUE(lp.pricing.effective_list.default), 4) AS dbu_cost
FROM system.billing.usage u
JOIN system.billing.list_prices lp
    ON u.sku_name = lp.sku_name
    AND lp.price_end_time IS NULL
    AND lp.cloud = 'AZURE'
WHERE u.workspace_id = {workspace_id}
  AND u.billing_origin_product = 'JOBS'
  AND u.identity_metadata.run_as = current_user()
  AND u.custom_tags['project'] = 'tpch-benchmark'
  AND u.usage_date >= current_date() - INTERVAL 7 DAYS
  AND u.usage_metadata.job_name LIKE '%tpch_benchmark_%'
GROUP BY ALL
ORDER BY job_name
""")
display(billing_df)

# Build the full cost summary
print("\n=== Full Cost Summary ===\n")
print(f"{'Compute Type':<30} | {'Exec (s)':>8} | {'DBUs':>8} | {'List':>6} | {'Disc.':>6} | {'DBU Cost':>9} | {'VM Cost':>8} | {'Total':>8}")
print("-" * 110)

for row in exec_df:
    ct = row["compute_type"]
    exec_s = row["total_exec_seconds"]

    # Find matching billing row
    billing_rows = billing_df.filter(billing_df.job_name.contains(ct)).collect()
    if billing_rows:
        b = billing_rows[0]
        dbus = float(b["total_dbus"])
        list_price = float(b["price_per_dbu"])
    else:
        dbus = 0
        list_price = 0

    discounted_price = apply_dbu_discount(list_price)
    dbu_cost = round(dbus * discounted_price, 4)
    vm_cost = compute_vm_cost(ct, exec_s)
    total_cost = dbu_cost + vm_cost

    print(f"{ct:<30} | {exec_s:>8.1f} | {dbus:>8.4f} | {list_price:>6.2f} | {discounted_price:>6.4f} | ${dbu_cost:>8.4f} | ${vm_cost:>7.4f} | ${total_cost:>7.4f}")

print(f"\nDiscounts: DBU {DBU_DISCOUNT:.0%}, Azure VM {AZURE_VM_DISCOUNT:.0%}")
print(f"VM pricing (list): {VM_PRICING}")
print(f"Nodes: {NUM_NODES} (1 driver + {NUM_NODES - 1} workers)")
print("VM cost uses execution time only (excludes cluster startup).")
print("Spot VM pricing is not further discounted (already market rate).")

# COMMAND ----------

# DBTITLE 1,List Prices Reference
list_prices_df = spark.sql("""
SELECT
    sku_name,
    ROUND(pricing.effective_list.default, 2) AS price_per_dbu,
    usage_unit
FROM system.billing.list_prices
WHERE cloud = 'AZURE'
  AND price_end_time IS NULL
  AND (
    sku_name LIKE '%JOBS_COMPUTE%'
    OR sku_name LIKE '%JOBS_SERVERLESS_COMPUTE_US_EAST%'
  )
ORDER BY sku_name
""")
display(list_prices_df)
