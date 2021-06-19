Select sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline;
Select avg(ArrDelay) as x , absolute_error(x),relative_error(x) from airline;
Select count(ArrDelay) as x , absolute_error(x),relative_error(x) from airline;
Select uniqueCarrier, sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.9;
Select uniqueCarrier, avg(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.9;
Select uniqueCarrier, count(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier;
Select uniqueCarrier, sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.9 order by uniqueCarrier desc;
Select uniqueCarrier, avg(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.9 order by uniqueCarrier desc;
Select uniqueCarrier, count(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier order by uniqueCarrier desc ;
Select uniqueCarrier, sum(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.90 order by x;
Select uniqueCarrier, avg(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier having relative_error(x) < 0.9 order by x ;
Select uniqueCarrier, count(ArrDelay) as x , absolute_error(x),relative_error(x) from airline group by uniqueCarrier order by count(ArrDelay);
Select sum(CRSDepTime) as x,sum(DepTime) as y ,relative_error(x),relative_error(y) from airline;
Select avg(CRSDepTime) as x,avg(DepTime) as y ,relative_error(x),relative_error(y) from airline;
Select count(CRSDepTime) as x,count(DepTime) as y ,relative_error(x),relative_error(y) from airline;