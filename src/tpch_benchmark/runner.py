"""Benchmark runner that executes TPC-H queries against serverless and classic warehouses."""

import json
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

from tpch_benchmark.queries import QUERIES, get_query


@dataclass
class QueryResult:
    query_id: str
    query_name: str
    warehouse_id: str
    warehouse_name: str
    warehouse_type: str
    execution_time_seconds: float
    status: str
    error: str | None = None
    row_count: int | None = None


@dataclass
class BenchmarkRun:
    run_id: str
    started_at: str
    warehouse_configs: list[dict] = field(default_factory=list)
    results: list[QueryResult] = field(default_factory=list)

    def to_dict(self) -> dict:
        return asdict(self)


def execute_query(
    client: WorkspaceClient,
    warehouse_id: str,
    warehouse_name: str,
    warehouse_type: str,
    query_id: str,
) -> QueryResult:
    """Execute a single TPC-H query and return timing results."""
    sql = get_query(query_id)
    query_name = QUERIES[query_id]["name"]

    start = time.perf_counter()
    try:
        response = client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=warehouse_id,
            wait_timeout="600s",
        )
        elapsed = time.perf_counter() - start

        if response.status and response.status.state == StatementState.SUCCEEDED:
            row_count = (
                response.manifest.total_row_count if response.manifest else None
            )
            return QueryResult(
                query_id=query_id,
                query_name=query_name,
                warehouse_id=warehouse_id,
                warehouse_name=warehouse_name,
                warehouse_type=warehouse_type,
                execution_time_seconds=round(elapsed, 3),
                status="SUCCESS",
                row_count=row_count,
            )
        else:
            error_msg = (
                response.status.error.message
                if response.status and response.status.error
                else "Unknown error"
            )
            return QueryResult(
                query_id=query_id,
                query_name=query_name,
                warehouse_id=warehouse_id,
                warehouse_name=warehouse_name,
                warehouse_type=warehouse_type,
                execution_time_seconds=round(elapsed, 3),
                status="FAILED",
                error=error_msg,
            )
    except Exception as e:
        elapsed = time.perf_counter() - start
        return QueryResult(
            query_id=query_id,
            query_name=query_name,
            warehouse_id=warehouse_id,
            warehouse_name=warehouse_name,
            warehouse_type=warehouse_type,
            execution_time_seconds=round(elapsed, 3),
            status="ERROR",
            error=str(e),
        )


def run_benchmark(
    client: WorkspaceClient,
    warehouses: list[dict],
    query_ids: list[str] | None = None,
    iterations: int = 1,
) -> BenchmarkRun:
    """Run TPC-H benchmark across multiple warehouses.

    Args:
        client: Authenticated Databricks WorkspaceClient.
        warehouses: List of dicts with keys: id, name, type (serverless|classic).
        query_ids: Optional subset of query IDs to run. Defaults to all 22.
        iterations: Number of times to run each query (for averaging).
    """
    if query_ids is None:
        query_ids = list(QUERIES.keys())

    run = BenchmarkRun(
        run_id=datetime.now().strftime("%Y%m%d_%H%M%S"),
        started_at=datetime.now().isoformat(),
        warehouse_configs=warehouses,
    )

    total = len(warehouses) * len(query_ids) * iterations
    completed = 0

    for wh in warehouses:
        print(f"\n{'='*60}")
        print(f"Warehouse: {wh['name']} ({wh['type']})")
        print(f"{'='*60}")

        for query_id in query_ids:
            for iteration in range(1, iterations + 1):
                completed += 1
                iter_label = f" [iter {iteration}/{iterations}]" if iterations > 1 else ""
                print(
                    f"  [{completed}/{total}] {query_id}: {QUERIES[query_id]['name']}{iter_label}...",
                    end=" ",
                    flush=True,
                )

                result = execute_query(
                    client=client,
                    warehouse_id=wh["id"],
                    warehouse_name=wh["name"],
                    warehouse_type=wh["type"],
                    query_id=query_id,
                )
                run.results.append(result)
                print(f"{result.execution_time_seconds}s ({result.status})")

    return run


def save_results(run: BenchmarkRun, path: str = "benchmark_results.json") -> str:
    """Save benchmark results to JSON."""
    with open(path, "w") as f:
        json.dump(run.to_dict(), f, indent=2)
    print(f"\nResults saved to {path}")
    return path
