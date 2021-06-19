---What times of day are profitable
--SELECT ROUND(SUM(FLOAT(Total_Amt)),2) AS amt,date_format(Trip_Pickup_DateTime,'dd.mm.yyyy'),vendor_name FROM NYCTAXI_BIGDATA GROUP BY 2,vendor_name ORDER BY amt LIMIT 10  ;
SELECT hour(nyctaxi.pickup_dateTime),sum(fare.fare_amount),sum(fare.surcharge),sum(fare.tip_amount) from nyctaxi,fare where nyctaxi.hack_license=fare.hack_license and nyctaxi.medallion=fare.medallion and nyctaxi.pickup_datetime=fare.pickup_datetime group by 1 order by sum(fare_amount) desc limit 30 with error;

--Find the total number of trips for each taxi
SELECT vendor_name, COUNT(*) AS number_of_trips FROM NYCTAXI_BIGDATA GROUP BY vendor_name;
SELECT medallion, COUNT(*) AS number_of_trips,absolute_error(number_of_trips),relative_error(number_of_trips) FROM NYCTAXI GROUP BY medallion order by number_of_trips desc limit 50 with error ;

---Aggregate the number of pickups at each long and lat coordinate in NY
--SELECT ROUND(Start_Lat, 4) as lat , ROUND(Start_Lon, 4) as lon, COUNT(*) as num_pickups FROM NYCTAXI_BIGDATA WHERE (Start_Lat BETWEEN 40.61 AND 40.91) AND (Start_Lon BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc;
SELECT ROUND(pickup_latitude, 4) as lat , ROUND(pickup_longitude, 4) as lon, COUNT(*) as num_pickups,absolute_error(num_pickups),relative_error(num_pickups) FROM NYCTAXI  WHERE (pickup_latitude BETWEEN 40.61 AND 40.91) AND (pickup_longitude BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc limit 50 with error 0.1;//doesnot work with default error

---Which location has most number of pickups and is profitable.
SELECT ROUND(pickup_latitude, 4) as lat , ROUND(pickup_longitude, 4) as lon, COUNT(*) as num_pickups ,ROUND(sum(total_amount),2) as totalAmt FROM NYCTAXI,FARE  WHERE (pickup_latitude BETWEEN 40.61 AND 40.91) AND (pickup_longitude BETWEEN -74.06 AND -73.77) and nyctaxi.hack_license=fare.hack_license and nyctaxi.medallion=fare.medallion and nyctaxi.pickup_datetime=fare.pickup_datetime GROUP BY 1,2 ORDER BY num_pickups desc with error;

---AVG TRIP DISTANCE PER TAXI
-- SELECT avg(trip_distance),vendor_name FROM NYCTAXI_BIGDATA GROUP BY vendor_name;
SELECT avg(trip_distance) as trip_distance,absolute_error(trip_distance),relative_error(trip_distance), medallion from nyctaxi group by medallion order by trip_distance,medallion desc limit 10 with error 0.2

---December 2013 Total Revenue
SELECT SUM(FLOAT(Total_Amt)) as sum_amt ,relative_error(sum_amt),FROM  NYCTAXI_BIGDATA WHERE cast(YEAR(TIMESTAMP(Trip_Pickup_DateTime)) as int ) = 2013 AND cast(MONTH(TIMESTAMP(Trip_Pickup_DateTime)) as int) = 12



