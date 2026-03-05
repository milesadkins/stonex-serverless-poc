"""TPC-H benchmark queries adapted for Databricks samples.tpch schema."""

CATALOG = "samples"
SCHEMA = "tpch"

QUERIES: dict[str, dict] = {
    "Q01": {
        "name": "Pricing Summary Report",
        "sql": """
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) AS sum_qty,
    sum(l_extendedprice) AS sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
    avg(l_quantity) AS avg_qty,
    avg(l_extendedprice) AS avg_price,
    avg(l_discount) AS avg_disc,
    count(*) AS count_order
FROM {catalog}.{schema}.lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL 90 DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
""",
    },
    "Q02": {
        "name": "Minimum Cost Supplier",
        "sql": """
SELECT
    s_acctbal, s_name, n_name, p_partkey, p_mfgr,
    s_address, s_phone, s_comment
FROM {catalog}.{schema}.part p
JOIN {catalog}.{schema}.partsupp ps ON p.p_partkey = ps.ps_partkey
JOIN {catalog}.{schema}.supplier s ON s.s_suppkey = ps.ps_suppkey
JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
JOIN {catalog}.{schema}.region r ON n.n_regionkey = r.r_regionkey
WHERE p_size = 15
    AND p_type LIKE '%BRASS'
    AND r_name = 'EUROPE'
    AND ps_supplycost = (
        SELECT min(ps_supplycost)
        FROM {catalog}.{schema}.partsupp ps2
        JOIN {catalog}.{schema}.supplier s2 ON s2.s_suppkey = ps2.ps_suppkey
        JOIN {catalog}.{schema}.nation n2 ON s2.s_nationkey = n2.n_nationkey
        JOIN {catalog}.{schema}.region r2 ON n2.n_regionkey = r2.r_regionkey
        WHERE p.p_partkey = ps2.ps_partkey AND r2.r_name = 'EUROPE'
    )
ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
LIMIT 100
""",
    },
    "Q03": {
        "name": "Shipping Priority",
        "sql": """
SELECT
    l_orderkey,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.orders o ON c.c_custkey = o.o_custkey
JOIN {catalog}.{schema}.lineitem l ON l.l_orderkey = o.o_orderkey
WHERE c_mktsegment = 'BUILDING'
    AND o_orderdate < DATE '1995-03-15'
    AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
""",
    },
    "Q04": {
        "name": "Order Priority Checking",
        "sql": """
SELECT
    o_orderpriority,
    count(*) AS order_count
FROM {catalog}.{schema}.orders o
WHERE o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL 3 MONTH
    AND EXISTS (
        SELECT 1 FROM {catalog}.{schema}.lineitem l
        WHERE l.l_orderkey = o.o_orderkey
            AND l_commitdate < l_receiptdate
    )
GROUP BY o_orderpriority
ORDER BY o_orderpriority
""",
    },
    "Q05": {
        "name": "Local Supplier Volume",
        "sql": """
SELECT
    n_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.orders o ON c.c_custkey = o.o_custkey
JOIN {catalog}.{schema}.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN {catalog}.{schema}.supplier s ON l.l_suppkey = s.s_suppkey AND c.c_nationkey = s.s_nationkey
JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
JOIN {catalog}.{schema}.region r ON n.n_regionkey = r.r_regionkey
WHERE r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1994-01-01' + INTERVAL 1 YEAR
GROUP BY n_name
ORDER BY revenue DESC
""",
    },
    "Q06": {
        "name": "Forecasting Revenue Change",
        "sql": """
SELECT
    sum(l_extendedprice * l_discount) AS revenue
FROM {catalog}.{schema}.lineitem
WHERE l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24
""",
    },
    "Q07": {
        "name": "Volume Shipping",
        "sql": """
SELECT
    supp_nation, cust_nation, l_year,
    sum(volume) AS revenue
FROM (
    SELECT
        n1.n_name AS supp_nation,
        n2.n_name AS cust_nation,
        year(l_shipdate) AS l_year,
        l_extendedprice * (1 - l_discount) AS volume
    FROM {catalog}.{schema}.supplier s
    JOIN {catalog}.{schema}.lineitem l ON s.s_suppkey = l.l_suppkey
    JOIN {catalog}.{schema}.orders o ON o.o_orderkey = l.l_orderkey
    JOIN {catalog}.{schema}.customer c ON c.c_custkey = o.o_custkey
    JOIN {catalog}.{schema}.nation n1 ON s.s_nationkey = n1.n_nationkey
    JOIN {catalog}.{schema}.nation n2 ON c.c_nationkey = n2.n_nationkey
    WHERE ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
        OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
        AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
) AS shipping
GROUP BY supp_nation, cust_nation, l_year
ORDER BY supp_nation, cust_nation, l_year
""",
    },
    "Q08": {
        "name": "National Market Share",
        "sql": """
SELECT
    o_year,
    sum(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END) / sum(volume) AS mkt_share
FROM (
    SELECT
        year(o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) AS volume,
        n2.n_name AS nation
    FROM {catalog}.{schema}.part p
    JOIN {catalog}.{schema}.lineitem l ON p.p_partkey = l.l_partkey
    JOIN {catalog}.{schema}.supplier s ON s.s_suppkey = l.l_suppkey
    JOIN {catalog}.{schema}.orders o ON o.o_orderkey = l.l_orderkey
    JOIN {catalog}.{schema}.customer c ON o.o_custkey = c.c_custkey
    JOIN {catalog}.{schema}.nation n1 ON c.c_nationkey = n1.n_nationkey
    JOIN {catalog}.{schema}.region r ON n1.n_regionkey = r.r_regionkey
    JOIN {catalog}.{schema}.nation n2 ON s.s_nationkey = n2.n_nationkey
    WHERE r_name = 'AMERICA'
        AND o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
        AND p_type = 'ECONOMY ANODIZED STEEL'
) AS all_nations
GROUP BY o_year
ORDER BY o_year
""",
    },
    "Q09": {
        "name": "Product Type Profit Measure",
        "sql": """
SELECT
    nation, o_year, sum(amount) AS sum_profit
FROM (
    SELECT
        n_name AS nation,
        year(o_orderdate) AS o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
    FROM {catalog}.{schema}.part p
    JOIN {catalog}.{schema}.lineitem l ON p.p_partkey = l.l_partkey
    JOIN {catalog}.{schema}.supplier s ON s.s_suppkey = l.l_suppkey
    JOIN {catalog}.{schema}.partsupp ps ON ps.ps_suppkey = l.l_suppkey AND ps.ps_partkey = l.l_partkey
    JOIN {catalog}.{schema}.orders o ON o.o_orderkey = l.l_orderkey
    JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
    WHERE p_name LIKE '%green%'
) AS profit
GROUP BY nation, o_year
ORDER BY nation, o_year DESC
""",
    },
    "Q10": {
        "name": "Returned Item Reporting",
        "sql": """
SELECT
    c_custkey, c_name,
    sum(l_extendedprice * (1 - l_discount)) AS revenue,
    c_acctbal, n_name, c_address, c_phone, c_comment
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.orders o ON c.c_custkey = o.o_custkey
JOIN {catalog}.{schema}.lineitem l ON l.l_orderkey = o.o_orderkey
JOIN {catalog}.{schema}.nation n ON c.c_nationkey = n.n_nationkey
WHERE o_orderdate >= DATE '1993-10-01'
    AND o_orderdate < DATE '1993-10-01' + INTERVAL 3 MONTH
    AND l_returnflag = 'R'
GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
ORDER BY revenue DESC
LIMIT 20
""",
    },
    "Q11": {
        "name": "Important Stock Identification",
        "sql": """
SELECT
    ps_partkey,
    sum(ps_supplycost * ps_availqty) AS value
FROM {catalog}.{schema}.partsupp ps
JOIN {catalog}.{schema}.supplier s ON ps.ps_suppkey = s.s_suppkey
JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
WHERE n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING sum(ps_supplycost * ps_availqty) > (
    SELECT sum(ps_supplycost * ps_availqty) * 0.0001
    FROM {catalog}.{schema}.partsupp ps2
    JOIN {catalog}.{schema}.supplier s2 ON ps2.ps_suppkey = s2.s_suppkey
    JOIN {catalog}.{schema}.nation n2 ON s2.s_nationkey = n2.n_nationkey
    WHERE n2.n_name = 'GERMANY'
)
ORDER BY value DESC
""",
    },
    "Q12": {
        "name": "Shipping Modes and Order Priority",
        "sql": """
SELECT
    l_shipmode,
    sum(CASE WHEN o_orderpriority = '1-URGENT' OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END) AS high_line_count,
    sum(CASE WHEN o_orderpriority <> '1-URGENT' AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END) AS low_line_count
FROM {catalog}.{schema}.orders o
JOIN {catalog}.{schema}.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE l_shipmode IN ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1994-01-01' + INTERVAL 1 YEAR
GROUP BY l_shipmode
ORDER BY l_shipmode
""",
    },
    "Q13": {
        "name": "Customer Distribution",
        "sql": """
SELECT
    c_count, count(*) AS custdist
FROM (
    SELECT c_custkey, count(o_orderkey) AS c_count
    FROM {catalog}.{schema}.customer c
    LEFT OUTER JOIN {catalog}.{schema}.orders o
        ON c.c_custkey = o.o_custkey
        AND o_comment NOT LIKE '%special%requests%'
    GROUP BY c_custkey
) AS c_orders
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC
""",
    },
    "Q14": {
        "name": "Promotion Effect",
        "sql": """
SELECT
    100.00 * sum(CASE WHEN p_type LIKE 'PROMO%' THEN l_extendedprice * (1 - l_discount) ELSE 0 END)
    / sum(l_extendedprice * (1 - l_discount)) AS promo_revenue
FROM {catalog}.{schema}.lineitem l
JOIN {catalog}.{schema}.part p ON l.l_partkey = p.p_partkey
WHERE l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL 1 MONTH
""",
    },
    "Q15": {
        "name": "Top Supplier",
        "sql": """
WITH revenue0 AS (
    SELECT
        l_suppkey AS supplier_no,
        sum(l_extendedprice * (1 - l_discount)) AS total_revenue
    FROM {catalog}.{schema}.lineitem
    WHERE l_shipdate >= DATE '1996-01-01'
        AND l_shipdate < DATE '1996-01-01' + INTERVAL 3 MONTH
    GROUP BY l_suppkey
)
SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
FROM {catalog}.{schema}.supplier s
JOIN revenue0 r ON s.s_suppkey = r.supplier_no
WHERE total_revenue = (SELECT max(total_revenue) FROM revenue0)
ORDER BY s_suppkey
""",
    },
    "Q16": {
        "name": "Parts/Supplier Relationship",
        "sql": """
SELECT
    p_brand, p_type, p_size,
    count(DISTINCT ps_suppkey) AS supplier_cnt
FROM {catalog}.{schema}.partsupp ps
JOIN {catalog}.{schema}.part p ON p.p_partkey = ps.ps_partkey
WHERE p_brand <> 'Brand#45'
    AND p_type NOT LIKE 'MEDIUM POLISHED%'
    AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
    AND ps_suppkey NOT IN (
        SELECT s_suppkey FROM {catalog}.{schema}.supplier
        WHERE s_comment LIKE '%Customer%Complaints%'
    )
GROUP BY p_brand, p_type, p_size
ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
""",
    },
    "Q17": {
        "name": "Small-Quantity-Order Revenue",
        "sql": """
SELECT
    sum(l_extendedprice) / 7.0 AS avg_yearly
FROM {catalog}.{schema}.lineitem l
JOIN {catalog}.{schema}.part p ON p.p_partkey = l.l_partkey
WHERE p_brand = 'Brand#23'
    AND p_container = 'MED BOX'
    AND l_quantity < (
        SELECT 0.2 * avg(l_quantity)
        FROM {catalog}.{schema}.lineitem l2
        WHERE l2.l_partkey = p.p_partkey
    )
""",
    },
    "Q18": {
        "name": "Large Volume Customer",
        "sql": """
SELECT
    c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
    sum(l_quantity)
FROM {catalog}.{schema}.customer c
JOIN {catalog}.{schema}.orders o ON c.c_custkey = o.o_custkey
JOIN {catalog}.{schema}.lineitem l ON o.o_orderkey = l.l_orderkey
WHERE o.o_orderkey IN (
    SELECT l_orderkey FROM {catalog}.{schema}.lineitem
    GROUP BY l_orderkey HAVING sum(l_quantity) > 300
)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
""",
    },
    "Q19": {
        "name": "Discounted Revenue",
        "sql": """
SELECT
    sum(l_extendedprice * (1 - l_discount)) AS revenue
FROM {catalog}.{schema}.lineitem l
JOIN {catalog}.{schema}.part p ON p.p_partkey = l.l_partkey
WHERE (
        p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
    OR (
        p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )
""",
    },
    "Q20": {
        "name": "Potential Part Promotion",
        "sql": """
SELECT s_name, s_address
FROM {catalog}.{schema}.supplier s
JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
WHERE s_suppkey IN (
    SELECT ps_suppkey
    FROM {catalog}.{schema}.partsupp ps
    WHERE ps_partkey IN (
        SELECT p_partkey FROM {catalog}.{schema}.part WHERE p_name LIKE 'forest%'
    )
    AND ps_availqty > (
        SELECT 0.5 * sum(l_quantity)
        FROM {catalog}.{schema}.lineitem l
        WHERE l.l_partkey = ps.ps_partkey
            AND l.l_suppkey = ps.ps_suppkey
            AND l_shipdate >= DATE '1994-01-01'
            AND l_shipdate < DATE '1994-01-01' + INTERVAL 1 YEAR
    )
)
AND n_name = 'CANADA'
ORDER BY s_name
""",
    },
    "Q21": {
        "name": "Suppliers Who Kept Orders Waiting",
        "sql": """
SELECT s_name, count(*) AS numwait
FROM {catalog}.{schema}.supplier s
JOIN {catalog}.{schema}.lineitem l1 ON s.s_suppkey = l1.l_suppkey
JOIN {catalog}.{schema}.orders o ON o.o_orderkey = l1.l_orderkey
JOIN {catalog}.{schema}.nation n ON s.s_nationkey = n.n_nationkey
WHERE o_orderstatus = 'F'
    AND l1.l_receiptdate > l1.l_commitdate
    AND EXISTS (
        SELECT 1 FROM {catalog}.{schema}.lineitem l2
        WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey
    )
    AND NOT EXISTS (
        SELECT 1 FROM {catalog}.{schema}.lineitem l3
        WHERE l3.l_orderkey = l1.l_orderkey
            AND l3.l_suppkey <> l1.l_suppkey
            AND l3.l_receiptdate > l3.l_commitdate
    )
    AND n_name = 'SAUDI ARABIA'
GROUP BY s_name
ORDER BY numwait DESC, s_name
LIMIT 100
""",
    },
    "Q22": {
        "name": "Global Sales Opportunity",
        "sql": """
SELECT
    cntrycode,
    count(*) AS numcust,
    sum(c_acctbal) AS totacctbal
FROM (
    SELECT
        substring(c_phone, 1, 2) AS cntrycode,
        c_acctbal
    FROM {catalog}.{schema}.customer c
    WHERE substring(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
        AND c_acctbal > (
            SELECT avg(c_acctbal)
            FROM {catalog}.{schema}.customer
            WHERE c_acctbal > 0.00
                AND substring(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
        )
        AND NOT EXISTS (
            SELECT 1 FROM {catalog}.{schema}.orders o WHERE o.o_custkey = c.c_custkey
        )
) AS custsale
GROUP BY cntrycode
ORDER BY cntrycode
""",
    },
}


def get_query(query_id: str) -> str:
    """Return a formatted TPC-H query ready for execution."""
    return QUERIES[query_id]["sql"].format(catalog=CATALOG, schema=SCHEMA).strip()


def list_queries() -> list[tuple[str, str]]:
    """Return list of (query_id, query_name) tuples."""
    return [(qid, q["name"]) for qid, q in QUERIES.items()]
