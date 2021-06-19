Select sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline with error;
Select avg(ArrDelay) as x , absolute_error(x),relative_error(x) from airline where uniqueCarrier = 'MQ' with error;
Select count(ArrDelay) as x , absolute_error(x),relative_error(x) from airline  with error;
Select sum(ArrDelay) as x ,uniqueCarrier, absolute_error(x),relative_error(x) from airline group by uniqueCarrier order by uniqueCarrier limit 10 with error;
Select avg(ArrDelay) as x ,uniqueCarrier, absolute_error(x),relative_error(x) from airline group by uniqueCarrier order by uniqueCarrier limit 10 with error;


