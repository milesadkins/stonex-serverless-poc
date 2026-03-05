# TPC-H Benchmark: Serverless vs Classic Jobs Compute

Compares the cost and performance of four Databricks compute configurations by running all 22 TPC-H queries against the built-in `samples.tpch` dataset.

## Compute Configurations

| Configuration | Compute Type | VM Type | Notes |
|---|---|---|---|
| Serverless Performance Optimized | Serverless (`PERFORMANCE_OPTIMIZED`) | N/A (managed) | Fastest serverless tier |
| Serverless Cost Optimized | Serverless (`STANDARD`) | N/A (managed) | Lower DBU rate |
| Classic On-Demand | Classic Jobs | Standard_D8s_v3 x3 | All nodes on-demand |
| Classic Spot | Classic Jobs | Standard_D8s_v3 x3 | 1 on-demand driver + 2 spot workers |

## Project Structure

```
databricks.yml              # Databricks Asset Bundle config (targets, variables)
resources/
  benchmark_job.yml         # Job definitions for all 4 compute configurations
notebooks/
  run_benchmark.py          # Benchmark notebook (queries, timing, cost analysis)
```

## Configuration

### Bundle Variables (`databricks.yml`)

These control the job infrastructure and are passed to the notebook as widget parameters. Override at deploy/run time with `--var`:

| Variable | Default | Description |
|---|---|---|
| `benchmark_run_id` | `default` | Unique identifier for this benchmark run |
| `results_catalog` | `main` | Unity Catalog catalog for the results table |
| `results_schema` | `default` | Schema for the results table |
| `node_type_id` | `Standard_D8s_v3` | Azure VM SKU for classic compute clusters |
| `num_workers` | `2` | Number of worker nodes for classic compute |
| `spark_version` | `15.4.x-scala2.12` | Databricks Runtime version for classic compute |

### Notebook Constants (`notebooks/run_benchmark.py`)

These are edited directly in the notebook's Configuration cell:

| Parameter | Default | Description |
|---|---|---|
| `CATALOG` | `samples` | Unity Catalog catalog containing TPC-H source data |
| `SCHEMA` | `tpch` | Schema containing TPC-H tables |
| `ITERATIONS` | `1` | Number of times to run each query |
| `VM_PRICING` | See notebook | Azure VM hourly rates (on-demand and spot) per instance type |
| `DBU_DISCOUNT` | `0.39` | Customer discount rate on Databricks DBUs (39%) |
| `AZURE_VM_DISCOUNT` | `0.60` | Customer discount rate on Azure VMs (60%) |

The results table is written to `<results_catalog>.<results_schema>.tpch_benchmark_results`. The classic cluster shape (`NODE_TYPE`, `NUM_NODES`) is derived automatically from the bundle variables -- no need to keep them in sync manually.

## Pricing Logic

The total cost for each compute configuration has two components:

### 1. DBU Cost (all configurations)

```
DBU Cost = total_dbus * list_price_per_dbu * (1 - DBU_DISCOUNT)
```

- **`total_dbus`**: Actual DBU consumption queried from `system.billing.usage`, filtered by job name and workspace.
- **`list_price_per_dbu`**: Current list price queried from `system.billing.list_prices`, filtered by SKU, cloud=AZURE, and active pricing (price_end_time IS NULL).
- **`DBU_DISCOUNT`**: Customer-negotiated discount applied on top of list price.

Relevant SKUs:
- Serverless: `PREMIUM_JOBS_SERVERLESS_COMPUTE_US_EAST` ($0.45/DBU list)
- Classic: `PREMIUM_JOBS_COMPUTE` ($0.30/DBU list)

### 2. Azure VM Cost (classic only)

```
VM Cost = num_nodes * hourly_rate * (1 - AZURE_VM_DISCOUNT) * (execution_seconds / 3600)
```

- **Serverless** configurations have **zero VM cost** -- infrastructure is fully managed by Databricks and included in the DBU price.
- **Classic on-demand**: All nodes (driver + workers) use the on-demand hourly rate, discounted by `AZURE_VM_DISCOUNT`.
- **Classic spot**: The driver uses the on-demand rate (discounted), workers use the spot rate (already at market price, no further discount).
- **Execution time only**: VM cost is calculated from query execution time, not wall-clock time. Cluster startup/teardown is excluded since those costs are fixed overhead independent of workload.
- **VM rates** are sourced from [Azure VM Pricing](https://azure.microsoft.com/en-us/pricing/details/virtual-machines/linux/) and configured in the `VM_PRICING` dict. These are not available in Databricks system tables.

### 3. Total Cost

```
Total Cost = DBU Cost + VM Cost
```

For serverless, Total Cost = DBU Cost (VM Cost is $0).

## Running the Benchmark

### Prerequisites

- [Databricks CLI](https://docs.databricks.com/dev-tools/cli/index.html) authenticated to the target workspace
- Workspace must have access to `samples.tpch` (available by default)
- System tables (`system.billing.usage`, `system.billing.list_prices`) must be accessible for cost analysis

### Deploy and Run

```bash
# Deploy the bundle (uses defaults from databricks.yml)
databricks bundle deploy --target dev

# Run all 4 benchmarks with a unique run ID
RUN_ID=$(date +%Y%m%d_%H%M%S)

databricks bundle run tpch_serverless_perf_optimized --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_serverless_cost_optimized --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_classic_on_demand         --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_classic_spot               --var benchmark_run_id=$RUN_ID
```

To customize the classic cluster or results location, pass `--var` overrides:

```bash
databricks bundle deploy --target dev \
  --var node_type_id=Standard_D8ds_v5 \
  --var num_workers=4 \
  --var results_catalog=my_catalog \
  --var results_schema=benchmarks
```

Each job runs the same 22 TPC-H queries, records per-query execution times to the results Delta table, and (for serverless runs) displays a cost analysis comparing all configurations that share the same `benchmark_run_id`.

### Viewing Results

After all 4 jobs complete, execution time results are immediately available in the results Delta table. However, **cost analysis depends on `system.billing.usage`, which has a lag of up to 2-3 hours** after job completion.

#### Step 1: Check execution times (available immediately)

Open any of the completed job runs in the Databricks UI. The **Execution Time Pivot** cell shows per-query times across all 4 configurations for the given `benchmark_run_id`. You can also query the results table directly:

```sql
SELECT compute_type, query_id, execution_time_seconds
FROM <results_catalog>.<results_schema>.tpch_benchmark_results
WHERE benchmark_run_id = '<your_run_id>'
ORDER BY query_id, compute_type
```

#### Step 2: Get cost analysis (after billing data is available)

The **Cost Analysis** and **List Prices Reference** cells query `system.billing.usage` and `system.billing.list_prices`. If the cost analysis shows $0 DBUs or missing data, billing hasn't been ingested yet.

To get the full cost breakdown after the billing lag:

1. Re-run one of the **serverless** jobs with the same `benchmark_run_id`:
   ```bash
   databricks bundle run tpch_serverless_perf_optimized --var benchmark_run_id=<your_run_id>
   ```
   The benchmark queries will re-execute (quickly), but the cost analysis cells will now pick up billing data from all 4 original runs.

2. Alternatively, copy the cost analysis SQL from the notebook into a **SQL warehouse** query editor and run it there -- no need to re-run the full benchmark.

> **Note**: Classic compute clusters may be blocked from querying system tables by workspace IP ACL rules. The cost analysis cells handle this gracefully. For best results, always review cost output from a **serverless** job run or a **SQL warehouse**.
