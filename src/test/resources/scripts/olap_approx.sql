elapsedtime on;

select uniquecarrier,sum(arrdelay) as x from airline group by uniquecarrier order by uniquecarrier;
select uniquecarrier,sum(arrdelay) as x, absolute_error(x),relative_error(x) from airline group by
uniquecarrier order by uniquecarrier with error;

set spark.sql.aqp.behavior=local_omit;

select uniquecarrier,sum(arrdelay) as x from airline group by uniquecarrier order by uniquecarrier;
select uniquecarrier,sum(arrdelay) as x, absolute_error(x),relative_error(x) from airline group by
uniquecarrier order by uniquecarrier with error 0.2;

set spark.sql.aqp.behavior=PARTIAL_RUN_ON_BASE_TABLE;

select uniquecarrier,sum(arrdelay) as x from airline group by uniquecarrier order by uniquecarrier;
select uniquecarrier,sum(arrdelay) as x, absolute_error(x),relative_error(x) from airline group by
uniquecarrier order by uniquecarrier with error 0.2;

set spark.sql.aqp.behavior=run_on_full_table;

select uniquecarrier,sum(arrdelay) as x from airline group by uniquecarrier order by uniquecarrier;
select uniquecarrier,sum(arrdelay) as x, absolute_error(x),relative_error(x) from airline group by
uniquecarrier order by uniquecarrier with error 0.2;