# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-H Benchmark: Serverless vs Classic Jobs Compute
# MAGIC
# MAGIC Runs all 22 TPC-H queries against `samples.tpch` using PySpark DataFrame API.
# MAGIC This notebook is deployed as 4 separate jobs (one per compute configuration) via Databricks Asset Bundles.
# MAGIC
# MAGIC **All configuration is in the first cell below.** See `README.md` for full pricing methodology.

# COMMAND ----------

# DBTITLE 1,Configuration
import time
from datetime import datetime
from pyspark.sql import functions as F

# --- Benchmark Settings ---
CATALOG = "samples"
SCHEMA = "tpch"
ITERATIONS = 1
RESULTS_TABLE = "hive_metastore.default.tpch_benchmark_results"

# --- Classic Compute Cluster (must match resources/benchmark_job.yml) ---
NODE_TYPE = "Standard_D8s_v3"
NUM_NODES = 3  # 1 driver + 2 workers

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

# Get parameters from job
dbutils.widgets.text("compute_type", "unknown")
dbutils.widgets.text("benchmark_run_id", "unknown")
compute_type = dbutils.widgets.get("compute_type")
benchmark_run_id = dbutils.widgets.get("benchmark_run_id")

try:
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
except Exception:
    cluster_id = "unknown"

# Get the job run ID from the context for billing correlation
try:
    context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    job_run_id = context.currentRunId().toString() if context.currentRunId() else "unknown"
except Exception:
    job_run_id = "unknown"

print(f"Compute type: {compute_type}")
print(f"Benchmark run ID: {benchmark_run_id}")
print(f"Cluster ID: {cluster_id}")
print(f"Job run ID: {job_run_id}")

# COMMAND ----------

# DBTITLE 1,Load Tables
lineitem = spark.table(f"{CATALOG}.{SCHEMA}.lineitem")
orders = spark.table(f"{CATALOG}.{SCHEMA}.orders")
customer = spark.table(f"{CATALOG}.{SCHEMA}.customer")
supplier = spark.table(f"{CATALOG}.{SCHEMA}.supplier")
nation = spark.table(f"{CATALOG}.{SCHEMA}.nation")
region = spark.table(f"{CATALOG}.{SCHEMA}.region")
part = spark.table(f"{CATALOG}.{SCHEMA}.part")
partsupp = spark.table(f"{CATALOG}.{SCHEMA}.partsupp")

# COMMAND ----------

# DBTITLE 1,TPC-H Queries as PySpark

def q01():
    """Pricing Summary Report"""
    return (
        lineitem
        .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01"), 90))
        .groupBy("l_returnflag", "l_linestatus")
        .agg(
            F.sum("l_quantity").alias("sum_qty"),
            F.sum("l_extendedprice").alias("sum_base_price"),
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
            F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax"))).alias("sum_charge"),
            F.avg("l_quantity").alias("avg_qty"),
            F.avg("l_extendedprice").alias("avg_price"),
            F.avg("l_discount").alias("avg_disc"),
            F.count("*").alias("count_order"),
        )
        .orderBy("l_returnflag", "l_linestatus")
    )


def q02():
    """Minimum Cost Supplier"""
    min_cost = (
        partsupp.alias("ps2")
        .join(supplier.alias("s2"), F.col("s2.s_suppkey") == F.col("ps2.ps_suppkey"))
        .join(nation.alias("n2"), F.col("s2.s_nationkey") == F.col("n2.n_nationkey"))
        .join(region.alias("r2"), F.col("n2.n_regionkey") == F.col("r2.r_regionkey"))
        .filter(F.col("r2.r_name") == "EUROPE")
        .groupBy(F.col("ps2.ps_partkey").alias("mc_partkey"))
        .agg(F.min("ps2.ps_supplycost").alias("min_supplycost"))
    )
    return (
        part.alias("p")
        .join(partsupp.alias("ps"), F.col("p.p_partkey") == F.col("ps.ps_partkey"))
        .join(supplier.alias("s"), F.col("s.s_suppkey") == F.col("ps.ps_suppkey"))
        .join(nation.alias("n"), F.col("s.s_nationkey") == F.col("n.n_nationkey"))
        .join(region.alias("r"), F.col("n.n_regionkey") == F.col("r.r_regionkey"))
        .join(min_cost, (F.col("p.p_partkey") == F.col("mc_partkey")) & (F.col("ps.ps_supplycost") == F.col("min_supplycost")), "inner")
        .filter(
            (F.col("p.p_size") == 15)
            & F.col("p.p_type").like("%BRASS")
            & (F.col("r.r_name") == "EUROPE")
        )
        .select("s.s_acctbal", "s.s_name", "n.n_name", "p.p_partkey", "p.p_mfgr", "s.s_address", "s.s_phone", "s.s_comment")
        .orderBy(F.col("s_acctbal").desc(), "n_name", "s_name", "p_partkey")
        .limit(100)
    )


def q03():
    """Shipping Priority"""
    return (
        customer.filter(F.col("c_mktsegment") == "BUILDING")
        .join(orders, F.col("c_custkey") == F.col("o_custkey"))
        .join(lineitem, F.col("l_orderkey") == F.col("o_orderkey"))
        .filter((F.col("o_orderdate") < "1995-03-15") & (F.col("l_shipdate") > "1995-03-15"))
        .groupBy("l_orderkey", "o_orderdate", "o_shippriority")
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
        .orderBy(F.col("revenue").desc(), "o_orderdate")
        .limit(10)
    )


def q04():
    """Order Priority Checking"""
    late_lineitems = (
        lineitem
        .filter(F.col("l_commitdate") < F.col("l_receiptdate"))
        .select("l_orderkey").distinct()
    )
    return (
        orders
        .filter(
            (F.col("o_orderdate") >= "1993-07-01")
            & (F.col("o_orderdate") < F.date_add(F.lit("1993-07-01"), 90))
        )
        .join(late_lineitems, orders.o_orderkey == late_lineitems.l_orderkey, "leftsemi")
        .groupBy("o_orderpriority")
        .agg(F.count("*").alias("order_count"))
        .orderBy("o_orderpriority")
    )


def q05():
    """Local Supplier Volume"""
    return (
        customer
        .join(orders, F.col("c_custkey") == F.col("o_custkey"))
        .join(lineitem, F.col("l_orderkey") == F.col("o_orderkey"))
        .join(supplier, (F.col("l_suppkey") == F.col("s_suppkey")) & (F.col("c_nationkey") == F.col("s_nationkey")))
        .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
        .join(region, F.col("n_regionkey") == F.col("r_regionkey"))
        .filter(
            (F.col("r_name") == "ASIA")
            & (F.col("o_orderdate") >= "1994-01-01")
            & (F.col("o_orderdate") < "1995-01-01")
        )
        .groupBy("n_name")
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
        .orderBy(F.col("revenue").desc())
    )


def q06():
    """Forecasting Revenue Change"""
    return (
        lineitem
        .filter(
            (F.col("l_shipdate") >= "1994-01-01")
            & (F.col("l_shipdate") < "1995-01-01")
            & (F.col("l_discount").between(0.05, 0.07))
            & (F.col("l_quantity") < 24)
        )
        .agg(F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue"))
    )


def q07():
    """Volume Shipping"""
    n1 = nation.alias("n1")
    n2 = nation.alias("n2")
    shipping = (
        supplier
        .join(lineitem, F.col("s_suppkey") == F.col("l_suppkey"))
        .join(orders, F.col("o_orderkey") == F.col("l_orderkey"))
        .join(customer, F.col("c_custkey") == F.col("o_custkey"))
        .join(n1, F.col("s_nationkey") == F.col("n1.n_nationkey"))
        .join(n2, F.col("c_nationkey") == F.col("n2.n_nationkey"))
        .filter(
            (
                (F.col("n1.n_name") == "FRANCE") & (F.col("n2.n_name") == "GERMANY")
                | (F.col("n1.n_name") == "GERMANY") & (F.col("n2.n_name") == "FRANCE")
            )
            & F.col("l_shipdate").between("1995-01-01", "1996-12-31")
        )
        .select(
            F.col("n1.n_name").alias("supp_nation"),
            F.col("n2.n_name").alias("cust_nation"),
            F.year("l_shipdate").alias("l_year"),
            (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume"),
        )
    )
    return (
        shipping
        .groupBy("supp_nation", "cust_nation", "l_year")
        .agg(F.sum("volume").alias("revenue"))
        .orderBy("supp_nation", "cust_nation", "l_year")
    )


def q08():
    """National Market Share"""
    n1 = nation.alias("n1")
    n2 = nation.alias("n2")
    all_nations = (
        part
        .join(lineitem, F.col("p_partkey") == F.col("l_partkey"))
        .join(supplier, F.col("s_suppkey") == F.col("l_suppkey"))
        .join(orders, F.col("o_orderkey") == F.col("l_orderkey"))
        .join(customer, F.col("o_custkey") == F.col("c_custkey"))
        .join(n1, F.col("c_nationkey") == F.col("n1.n_nationkey"))
        .join(region, F.col("n1.n_regionkey") == F.col("r_regionkey"))
        .join(n2, F.col("s_nationkey") == F.col("n2.n_nationkey"))
        .filter(
            (F.col("r_name") == "AMERICA")
            & F.col("o_orderdate").between("1995-01-01", "1996-12-31")
            & (F.col("p_type") == "ECONOMY ANODIZED STEEL")
        )
        .select(
            F.year("o_orderdate").alias("o_year"),
            (F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("volume"),
            F.col("n2.n_name").alias("nation"),
        )
    )
    return (
        all_nations
        .groupBy("o_year")
        .agg(
            (F.sum(F.when(F.col("nation") == "BRAZIL", F.col("volume")).otherwise(0)) / F.sum("volume")).alias("mkt_share")
        )
        .orderBy("o_year")
    )


def q09():
    """Product Type Profit Measure"""
    profit = (
        part.filter(F.col("p_name").like("%green%"))
        .join(lineitem, F.col("p_partkey") == F.col("l_partkey"))
        .join(supplier, F.col("s_suppkey") == F.col("l_suppkey"))
        .join(partsupp, (F.col("ps_suppkey") == F.col("l_suppkey")) & (F.col("ps_partkey") == F.col("l_partkey")))
        .join(orders, F.col("o_orderkey") == F.col("l_orderkey"))
        .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
        .select(
            F.col("n_name").alias("nation"),
            F.year("o_orderdate").alias("o_year"),
            (F.col("l_extendedprice") * (1 - F.col("l_discount")) - F.col("ps_supplycost") * F.col("l_quantity")).alias("amount"),
        )
    )
    return (
        profit
        .groupBy("nation", "o_year")
        .agg(F.sum("amount").alias("sum_profit"))
        .orderBy("nation", F.col("o_year").desc())
    )


def q10():
    """Returned Item Reporting"""
    return (
        customer
        .join(orders, F.col("c_custkey") == F.col("o_custkey"))
        .join(lineitem, F.col("l_orderkey") == F.col("o_orderkey"))
        .join(nation, F.col("c_nationkey") == F.col("n_nationkey"))
        .filter(
            (F.col("o_orderdate") >= "1993-10-01")
            & (F.col("o_orderdate") < F.date_add(F.lit("1993-10-01"), 90))
            & (F.col("l_returnflag") == "R")
        )
        .groupBy("c_custkey", "c_name", "c_acctbal", "c_phone", "n_name", "c_address", "c_comment")
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
        .orderBy(F.col("revenue").desc())
        .limit(20)
    )


def q11():
    """Important Stock Identification"""
    threshold = (
        partsupp.alias("ps2")
        .join(supplier.alias("s2"), F.col("ps2.ps_suppkey") == F.col("s2.s_suppkey"))
        .join(nation.alias("n2"), F.col("s2.s_nationkey") == F.col("n2.n_nationkey"))
        .filter(F.col("n2.n_name") == "GERMANY")
        .agg((F.sum(F.col("ps2.ps_supplycost") * F.col("ps2.ps_availqty")) * 0.0001).alias("threshold"))
        .collect()[0]["threshold"]
    )
    return (
        partsupp
        .join(supplier, F.col("ps_suppkey") == F.col("s_suppkey"))
        .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
        .filter(F.col("n_name") == "GERMANY")
        .groupBy("ps_partkey")
        .agg(F.sum(F.col("ps_supplycost") * F.col("ps_availqty")).alias("value"))
        .filter(F.col("value") > threshold)
        .orderBy(F.col("value").desc())
    )


def q12():
    """Shipping Modes and Order Priority"""
    return (
        orders
        .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
        .filter(
            F.col("l_shipmode").isin("MAIL", "SHIP")
            & (F.col("l_commitdate") < F.col("l_receiptdate"))
            & (F.col("l_shipdate") < F.col("l_commitdate"))
            & (F.col("l_receiptdate") >= "1994-01-01")
            & (F.col("l_receiptdate") < "1995-01-01")
        )
        .groupBy("l_shipmode")
        .agg(
            F.sum(F.when(F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).alias("high_line_count"),
            F.sum(F.when(~F.col("o_orderpriority").isin("1-URGENT", "2-HIGH"), 1).otherwise(0)).alias("low_line_count"),
        )
        .orderBy("l_shipmode")
    )


def q13():
    """Customer Distribution"""
    c_orders = (
        customer
        .join(
            orders.filter(~F.col("o_comment").like("%special%requests%")),
            F.col("c_custkey") == F.col("o_custkey"),
            "left",
        )
        .groupBy("c_custkey")
        .agg(F.count("o_orderkey").alias("c_count"))
    )
    return (
        c_orders
        .groupBy("c_count")
        .agg(F.count("*").alias("custdist"))
        .orderBy(F.col("custdist").desc(), F.col("c_count").desc())
    )


def q14():
    """Promotion Effect"""
    return (
        lineitem
        .join(part, F.col("l_partkey") == F.col("p_partkey"))
        .filter((F.col("l_shipdate") >= "1995-09-01") & (F.col("l_shipdate") < "1995-10-01"))
        .agg(
            (
                F.lit(100.0)
                * F.sum(F.when(F.col("p_type").like("PROMO%"), F.col("l_extendedprice") * (1 - F.col("l_discount"))).otherwise(0))
                / F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")))
            ).alias("promo_revenue")
        )
    )


def q15():
    """Top Supplier"""
    revenue0 = (
        lineitem
        .filter((F.col("l_shipdate") >= "1996-01-01") & (F.col("l_shipdate") < "1996-04-01"))
        .groupBy(F.col("l_suppkey").alias("supplier_no"))
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("total_revenue"))
    )
    max_revenue = revenue0.agg(F.max("total_revenue").alias("max_rev")).collect()[0]["max_rev"]
    return (
        supplier
        .join(revenue0, F.col("s_suppkey") == F.col("supplier_no"))
        .filter(F.col("total_revenue") == max_revenue)
        .select("s_suppkey", "s_name", "s_address", "s_phone", "total_revenue")
        .orderBy("s_suppkey")
    )


def q16():
    """Parts/Supplier Relationship"""
    complaint_suppliers = (
        supplier
        .filter(F.col("s_comment").like("%Customer%Complaints%"))
        .select("s_suppkey")
    )
    return (
        partsupp
        .join(part, F.col("ps_partkey") == F.col("p_partkey"))
        .filter(
            (F.col("p_brand") != "Brand#45")
            & ~F.col("p_type").like("MEDIUM POLISHED%")
            & F.col("p_size").isin(49, 14, 23, 45, 19, 3, 36, 9)
        )
        .join(complaint_suppliers, F.col("ps_suppkey") == F.col("s_suppkey"), "leftanti")
        .groupBy("p_brand", "p_type", "p_size")
        .agg(F.countDistinct("ps_suppkey").alias("supplier_cnt"))
        .orderBy(F.col("supplier_cnt").desc(), "p_brand", "p_type", "p_size")
    )


def q17():
    """Small-Quantity-Order Revenue"""
    avg_qty = (
        lineitem.alias("l2")
        .groupBy("l_partkey")
        .agg((F.lit(0.2) * F.avg("l_quantity")).alias("avg_qty"))
    )
    return (
        lineitem.alias("l")
        .join(part, F.col("l.l_partkey") == F.col("p_partkey"))
        .join(avg_qty, F.col("l.l_partkey") == avg_qty.l_partkey)
        .filter(
            (F.col("p_brand") == "Brand#23")
            & (F.col("p_container") == "MED BOX")
            & (F.col("l.l_quantity") < F.col("avg_qty"))
        )
        .agg((F.sum("l.l_extendedprice") / 7.0).alias("avg_yearly"))
    )


def q18():
    """Large Volume Customer"""
    large_orders = (
        lineitem
        .groupBy("l_orderkey")
        .agg(F.sum("l_quantity").alias("total_qty"))
        .filter(F.col("total_qty") > 300)
        .select("l_orderkey")
    )
    return (
        customer
        .join(orders, F.col("c_custkey") == F.col("o_custkey"))
        .join(lineitem, F.col("o_orderkey") == F.col("l_orderkey"))
        .join(large_orders, orders.o_orderkey == large_orders.l_orderkey, "leftsemi")
        .groupBy("c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice")
        .agg(F.sum("l_quantity").alias("sum_qty"))
        .orderBy(F.col("o_totalprice").desc(), "o_orderdate")
        .limit(100)
    )


def q19():
    """Discounted Revenue"""
    base = lineitem.join(part, F.col("l_partkey") == F.col("p_partkey"))
    cond1 = (
        (F.col("p_brand") == "Brand#12")
        & F.col("p_container").isin("SM CASE", "SM BOX", "SM PACK", "SM PKG")
        & (F.col("l_quantity") >= 1) & (F.col("l_quantity") <= 11)
        & F.col("p_size").between(1, 5)
        & F.col("l_shipmode").isin("AIR", "AIR REG")
        & (F.col("l_shipinstruct") == "DELIVER IN PERSON")
    )
    cond2 = (
        (F.col("p_brand") == "Brand#23")
        & F.col("p_container").isin("MED BAG", "MED BOX", "MED PKG", "MED PACK")
        & (F.col("l_quantity") >= 10) & (F.col("l_quantity") <= 20)
        & F.col("p_size").between(1, 10)
        & F.col("l_shipmode").isin("AIR", "AIR REG")
        & (F.col("l_shipinstruct") == "DELIVER IN PERSON")
    )
    cond3 = (
        (F.col("p_brand") == "Brand#34")
        & F.col("p_container").isin("LG CASE", "LG BOX", "LG PACK", "LG PKG")
        & (F.col("l_quantity") >= 20) & (F.col("l_quantity") <= 30)
        & F.col("p_size").between(1, 15)
        & F.col("l_shipmode").isin("AIR", "AIR REG")
        & (F.col("l_shipinstruct") == "DELIVER IN PERSON")
    )
    return (
        base.filter(cond1 | cond2 | cond3)
        .agg(F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue"))
    )


def q20():
    """Potential Part Promotion"""
    forest_parts = part.filter(F.col("p_name").like("forest%")).select("p_partkey")
    half_qty = (
        lineitem
        .filter((F.col("l_shipdate") >= "1994-01-01") & (F.col("l_shipdate") < "1995-01-01"))
        .groupBy("l_partkey", "l_suppkey")
        .agg((F.lit(0.5) * F.sum("l_quantity")).alias("half_sum_qty"))
    )
    eligible_suppliers = (
        partsupp
        .join(forest_parts, F.col("ps_partkey") == F.col("p_partkey"), "leftsemi")
        .join(half_qty, (F.col("ps_partkey") == F.col("l_partkey")) & (F.col("ps_suppkey") == F.col("l_suppkey")))
        .filter(F.col("ps_availqty") > F.col("half_sum_qty"))
        .select("ps_suppkey").distinct()
    )
    return (
        supplier
        .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
        .filter(F.col("n_name") == "CANADA")
        .join(eligible_suppliers, F.col("s_suppkey") == F.col("ps_suppkey"), "leftsemi")
        .select("s_name", "s_address")
        .orderBy("s_name")
    )


def q21():
    """Suppliers Who Kept Orders Waiting"""
    l1 = lineitem.alias("l1")
    multi_supplier_orders = (
        lineitem.alias("l2")
        .select("l_orderkey", "l_suppkey").distinct()
        .groupBy("l_orderkey").agg(F.countDistinct("l_suppkey").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .select("l_orderkey")
    )
    late_other_supplier = (
        lineitem.alias("l3")
        .filter(F.col("l3.l_receiptdate") > F.col("l3.l_commitdate"))
        .select(F.col("l3.l_orderkey"), F.col("l3.l_suppkey"))
    )
    return (
        supplier
        .join(l1, F.col("s_suppkey") == F.col("l1.l_suppkey"))
        .join(orders, F.col("o_orderkey") == F.col("l1.l_orderkey"))
        .join(nation, F.col("s_nationkey") == F.col("n_nationkey"))
        .filter(
            (F.col("o_orderstatus") == "F")
            & (F.col("l1.l_receiptdate") > F.col("l1.l_commitdate"))
            & (F.col("n_name") == "SAUDI ARABIA")
        )
        .join(multi_supplier_orders, F.col("l1.l_orderkey") == multi_supplier_orders.l_orderkey, "leftsemi")
        .join(
            late_other_supplier,
            (F.col("l1.l_orderkey") == late_other_supplier.l_orderkey)
            & (F.col("l1.l_suppkey") != late_other_supplier.l_suppkey),
            "leftanti",
        )
        .groupBy("s_name")
        .agg(F.count("*").alias("numwait"))
        .orderBy(F.col("numwait").desc(), "s_name")
        .limit(100)
    )


def q22():
    """Global Sales Opportunity"""
    country_codes = ["13", "31", "23", "29", "30", "18", "17"]
    avg_bal = (
        customer
        .filter(
            F.substring("c_phone", 1, 2).isin(*country_codes)
            & (F.col("c_acctbal") > 0.00)
        )
        .agg(F.avg("c_acctbal").alias("avg_acctbal"))
        .collect()[0]["avg_acctbal"]
    )
    customers_with_orders = orders.select("o_custkey").distinct()
    return (
        customer
        .filter(
            F.substring("c_phone", 1, 2).isin(*country_codes)
            & (F.col("c_acctbal") > avg_bal)
        )
        .join(customers_with_orders, F.col("c_custkey") == F.col("o_custkey"), "leftanti")
        .select(F.substring("c_phone", 1, 2).alias("cntrycode"), "c_acctbal")
        .groupBy("cntrycode")
        .agg(F.count("*").alias("numcust"), F.sum("c_acctbal").alias("totacctbal"))
        .orderBy("cntrycode")
    )


QUERIES = {
    "Q01": {"name": "Pricing Summary Report", "func": q01},
    "Q02": {"name": "Minimum Cost Supplier", "func": q02},
    "Q03": {"name": "Shipping Priority", "func": q03},
    "Q04": {"name": "Order Priority Checking", "func": q04},
    "Q05": {"name": "Local Supplier Volume", "func": q05},
    "Q06": {"name": "Forecasting Revenue Change", "func": q06},
    "Q07": {"name": "Volume Shipping", "func": q07},
    "Q08": {"name": "National Market Share", "func": q08},
    "Q09": {"name": "Product Type Profit Measure", "func": q09},
    "Q10": {"name": "Returned Item Reporting", "func": q10},
    "Q11": {"name": "Important Stock Identification", "func": q11},
    "Q12": {"name": "Shipping Modes and Order Priority", "func": q12},
    "Q13": {"name": "Customer Distribution", "func": q13},
    "Q14": {"name": "Promotion Effect", "func": q14},
    "Q15": {"name": "Top Supplier", "func": q15},
    "Q16": {"name": "Parts/Supplier Relationship", "func": q16},
    "Q17": {"name": "Small-Quantity-Order Revenue", "func": q17},
    "Q18": {"name": "Large Volume Customer", "func": q18},
    "Q19": {"name": "Discounted Revenue", "func": q19},
    "Q20": {"name": "Potential Part Promotion", "func": q20},
    "Q21": {"name": "Suppliers Who Kept Orders Waiting", "func": q21},
    "Q22": {"name": "Global Sales Opportunity", "func": q22},
}

# COMMAND ----------

# DBTITLE 1,Run Benchmark
results = []

for query_id, query_info in QUERIES.items():
    for iteration in range(1, ITERATIONS + 1):
        iter_label = f" [iter {iteration}/{ITERATIONS}]" if ITERATIONS > 1 else ""
        print(f"Running {query_id}: {query_info['name']}{iter_label}...", end=" ", flush=True)

        try:
            spark.catalog.clearCache()
        except Exception:
            pass

        start = time.perf_counter()
        try:
            df = query_info["func"]()
            row_count = df.count()
            elapsed = time.perf_counter() - start
            status = "SUCCESS"
            error = None
        except Exception as e:
            elapsed = time.perf_counter() - start
            row_count = None
            status = "ERROR"
            error = str(e)

        results.append({
            "query_id": query_id,
            "query_name": query_info["name"],
            "compute_type": compute_type,
            "cluster_id": cluster_id,
            "benchmark_run_id": benchmark_run_id,
            "iteration": iteration,
            "execution_time_seconds": round(elapsed, 3),
            "row_count": row_count,
            "status": status,
            "error": error,
            "timestamp": datetime.now().isoformat(),
        })
        print(f"{elapsed:.3f}s ({status})")

print(f"\nCompleted {len(results)} query runs.")

# COMMAND ----------

# DBTITLE 1,Save Results
import pandas as pd

results_df = spark.createDataFrame(pd.DataFrame(results))
display(results_df)

output_table = RESULTS_TABLE
results_df.write.mode("append").option("mergeSchema", "true").saveAsTable(output_table)
print(f"Results appended to {output_table}")

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
# Joins benchmark execution times with actual DBU consumption from system.billing.usage
# and list prices from system.billing.list_prices.
# NOTE: system.billing.usage has a lag of up to a few hours after job completion.
# This cell is non-fatal — if system tables are inaccessible, it prints a message and continues.

try:
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
        succeeded = row["succeeded"]
        num_q = row["num_queries"]

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

except Exception as e:
    print(f"Cost analysis skipped (system tables not accessible from this compute): {e}")
    print("Run this cell from a SQL warehouse or after billing data is ingested.")

# COMMAND ----------

# DBTITLE 1,List Prices Reference
# Show current list prices for relevant SKUs from system.billing.list_prices
try:
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
except Exception as e:
    print(f"List prices skipped: {e}")
