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

All tunable parameters live in the **Configuration** cell at the top of `notebooks/run_benchmark.py`:

| Parameter | Default | Description |
|---|---|---|
| `CATALOG` | `samples` | Unity Catalog catalog containing TPC-H data |
| `SCHEMA` | `tpch` | Schema containing TPC-H tables |
| `ITERATIONS` | `1` | Number of times to run each query |
| `RESULTS_TABLE` | `hive_metastore.default.tpch_benchmark_results` | Delta table for storing results |
| `NUM_NODES` | `3` | Total nodes for classic compute (1 driver + 2 workers) |
| `VM_PRICING` | See notebook | Azure VM hourly rates (on-demand and spot) per instance type |
| `DBU_DISCOUNT` | `0.39` | Customer discount rate on Databricks DBUs (39%) |
| `AZURE_VM_DISCOUNT` | `0.60` | Customer discount rate on Azure VMs (60%) |

Classic compute cluster shape is defined in `resources/benchmark_job.yml` (node type, worker count, spot settings). If you change the cluster there, update `NUM_NODES` and `VM_PRICING` in the notebook to match.

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
# Deploy the bundle
databricks bundle deploy --target dev

# Run all 4 benchmarks with a unique run ID
RUN_ID=$(date +%Y%m%d_%H%M%S)

databricks bundle run tpch_serverless_perf_optimized --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_serverless_cost_optimized --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_classic_on_demand         --var benchmark_run_id=$RUN_ID
databricks bundle run tpch_classic_spot               --var benchmark_run_id=$RUN_ID
```

Each job runs the same 22 TPC-H queries, records per-query execution times to the results Delta table, and (for serverless runs) displays a cost analysis comparing all configurations that share the same `benchmark_run_id`.

### Viewing Results

After all 4 jobs complete, the cost analysis cells in the notebook output show:
1. **Execution time pivot** -- per-query times across all 4 configurations
2. **Full cost summary** -- DBUs consumed, list/discounted prices, VM costs, and totals
3. **List prices reference** -- current DBU prices from system tables

> **Note**: `system.billing.usage` data may lag up to a few hours after job completion. If the cost analysis cell shows $0 DBUs, wait and re-run the analysis cells. Classic compute clusters may be blocked from querying system tables by IP ACL rules -- the cost analysis cells handle this gracefully and can be re-run from a serverless job or SQL warehouse.
