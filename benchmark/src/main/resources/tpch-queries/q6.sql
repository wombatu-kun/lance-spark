-- Source: Apache Kyuubi kyuubi-spark-connector-tpch test resources (Apache 2.0 licensed)

select
    round(sum(l_extendedprice * l_discount), 2) as revenue
from
    lineitem
where
    l_shipdate >= date '1994-01-01'
    and l_shipdate < date '1994-01-01' + interval '1' year
    and l_discount between .06 - 0.01 and .06 + 0.01
    and l_quantity < 24
