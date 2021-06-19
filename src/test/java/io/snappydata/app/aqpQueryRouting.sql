select uniquecarrier from airline where arrdelay > 5 group by uniquecarrier order by uniquecarrier;
select uniquecarrier,origin,dest,count(distinct cancelled) from airline where arrdelay > 5 group by uniquecarrier,origin,dest order by uniquecarrier limit 20 with error;
select uniquecarrier, avg(arrdelay) as x,absolute_error(x),relative_error(x)  from airline where uniquecarrier = 'HA' group by uniquecarrier order by uniquecarrier with error 0.2 behavior 'local_omit';
select uniquecarrier, sum(arrdelay) as x,absolute_error(x),relative_error(x)  from airline group by uniquecarrier order by uniquecarrier with error 0.2 behavior 'PARTIAL_RUN_ON_BASE_TABLE';
select uniquecarrier, sum(arrdelay) as x,absolute_error(x),relative_error(x)  from airline group by uniquecarrier order by uniquecarrier with error 0.2 behavior 'RUN_ON_FULL_TABLE';
select sum(arrdelay) as x ,max(arrdelay) ,absolute_error(x),relative_error(x) from airline with error;
select uniquecarrier, sum(arrdelay) from airline group by uniquecarrier order by uniquecarrier with error;