-- Source: Apache Kyuubi kyuubi-spark-connector-tpch test resources (Apache 2.0 licensed)

select
    l_returnflag,
    l_linestatus,
    round(sum(l_quantity), 2) as sum_qty,
    round(sum(l_extendedprice), 2) as sum_base_price,
    round(sum(l_extendedprice * (1 - l_discount)), 2) as sum_disc_price,
    round(sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)), 2) as sum_charge,
    round(avg(l_quantity), 2) as avg_qty,
    round(avg(l_extendedprice), 2) as avg_price,
    round(avg(l_discount), 2) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    l_shipdate <= date '1998-12-01' - interval '90' day
group by
    l_returnflag,
    l_linestatus
order by
    l_returnflag,
    l_linestatus
