elapsedtime on;
--- Create NYCTaxi table with 157 million records
DROP TABLE IF EXISTS STAGING_NYCTAXI ;
DROP TABLE IF EXISTS NYCTAXI ;
CREATE EXTERNAL TABLE STAGING_NYCTAXI
 USING parquet OPTIONS(path ':dataLocation/nytaxitripdata_cleaned');
 CREATE TABLE NYCTAXI USING column OPTIONS(buckets '11',redundancy '1') AS (select * from STAGING_NYCTAXI);

--- Create fare table
DROP TABLE IF EXISTS STAGING_FARETAXI ;
DROP TABLE IF EXISTS FARE ;
CREATE EXTERNAL TABLE STAGING_FARETAXI
  USING parquet OPTIONS(path ':dataLocation/nyctaxifaredata_cleaned');
CREATE TABLE FARE USING column OPTIONS(buckets '11',redundancy '1') AS (select * from STAGING_FARETAXI);

 DROP TABLE IF EXISTS NYCTAXI_SAMPLEHACKLICENSE ;
 CREATE SAMPLE TABLE NYCTAXI_SAMPLEHACKLICENSE ON NYCTAXI
  OPTIONS(
   qcs 'hack_license',
   fraction '0.01',
    strataReservoirSize '50');

DROP TABLE IF EXISTS NYCTAXI_SAMPLEHOUR ;
CREATE SAMPLE TABLE NYCTAXI_SAMPLEHOUR ON NYCTAXI
  OPTIONS(
   qcs 'hour(pickup_dateTime)',
   fraction '0.01',
    strataReservoirSize '50');

DROP TABLE IF EXISTS NYCTAXI_SAMPLEMEDALLION ;
CREATE SAMPLE TABLE NYCTAXI_SAMPLEMEDALLION ON NYCTAXI
  OPTIONS(
    qcs 'medallion',
    fraction '0.01',
    strataReservoirSize '50');
