# Databricks notebook source
# MAGIC %md
# MAGIC # TPC-H Benchmark: Serverless vs Classic Jobs Compute
# MAGIC
# MAGIC Runs all 22 TPC-H queries against `samples.tpch` using PySpark,
# MAGIC timing each query execution. Deploy as two jobs — one serverless,
# MAGIC one classic with `Standard_D8ds_v5` — then compare results.

# COMMAND ----------

# DBTITLE 1,Configuration
import time
from datetime import datetime
from pyspark.sql import functions as F

CATALOG = "samples"
SCHEMA = "tpch"
ITERATIONS = 1

# Detect compute type
try:
    cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "unknown")
except Exception:
    cluster_id = "unknown"

try:
    is_serverless = spark.conf.get("spark.databricks.isServerless", "false")
    compute_type = "serverless" if is_serverless.lower() == "true" else "classic"
except Exception:
    try:
        cluster_tags = spark.conf.get("spark.databricks.clusterUsageTags.clusterAllTags", "")
        compute_type = "serverless" if "Serverless" in cluster_tags else "classic"
    except Exception:
        compute_type = "unknown"

print(f"Compute type: {compute_type}")
print(f"Cluster ID: {cluster_id}")

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

output_table = "default.tpch_benchmark_results"
results_df.write.mode("append").saveAsTable(output_table)
print(f"Results appended to {output_table}")

# COMMAND ----------

# DBTITLE 1,Compare Results (run after both jobs complete)
comparison_df = spark.sql("""
SELECT query_id, query_name, compute_type,
    round(avg(execution_time_seconds), 3) AS avg_time_seconds,
    round(min(execution_time_seconds), 3) AS min_time_seconds,
    round(max(execution_time_seconds), 3) AS max_time_seconds,
    count(*) AS num_runs
FROM default.tpch_benchmark_results
WHERE status = 'SUCCESS'
GROUP BY query_id, query_name, compute_type
ORDER BY query_id, compute_type
""")
display(comparison_df)

# COMMAND ----------

# DBTITLE 1,Speedup Summary
speedup_df = spark.sql("""
WITH by_type AS (
    SELECT query_id, query_name, compute_type, avg(execution_time_seconds) AS avg_time
    FROM default.tpch_benchmark_results WHERE status = 'SUCCESS'
    GROUP BY query_id, query_name, compute_type
)
SELECT c.query_id, c.query_name,
    round(c.avg_time, 3) AS classic_seconds,
    round(s.avg_time, 3) AS serverless_seconds,
    round(c.avg_time / s.avg_time, 2) AS speedup,
    round((c.avg_time - s.avg_time) / c.avg_time * 100, 1) AS pct_faster
FROM by_type c JOIN by_type s ON c.query_id = s.query_id
WHERE c.compute_type = 'classic' AND s.compute_type = 'serverless'
ORDER BY c.query_id
""")
display(speedup_df)
