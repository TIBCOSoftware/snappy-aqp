--- Create climateChange column table
CREATE EXTERNAL TABLE climateChange_staging
USING com.databricks.spark.csv OPTIONS(path ':dataLocation/climateChange/data/climate1788-2011.csv', header 'true', inferSchema 'true', nullValue 'NULL', maxCharsPerColumn '4096');
CREATE TABLE climateChange
 USING column options(buckets '32',redundancy '1')
  AS (SELECT * FROM climateChange_staging);

--- Create a sample table
CREATE SAMPLE TABLE climateChangeSampleTable ON climateChange
  OPTIONS(buckets '7',
   qcs 'element',
   fraction '0.01',
   strataReservoirSize '50');
  --  AS (SELECT * FROM climateChange);

--- Create a view on base table.
CREATE VIEW climateChange_View AS SELECT ID AS stationId,
 IF( ELEMENT='TMAX', data_value, NULL )
  AS tmax,IF( ELEMENT='TMIN', data_value, NULL )
   AS tmin,CAST(substr(ymd, 0, 4) AS INT)
  AS year FROM CLIMATECHANGE WHERE ELEMENT IN ('TMIN','TMAX');
