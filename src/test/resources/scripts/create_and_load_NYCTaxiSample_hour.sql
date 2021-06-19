DROP TABLE IF EXISTS NYCTAXI_SAMPLEHOUR ;
CREATE SAMPLE TABLE NYCTAXI_SAMPLEHOUR ON NYCTAXI
  OPTIONS(    buckets '7', qcs 'hour(pickup_dateTime)', fraction '0.01', strataReservoirSize '50');
  --AS (SELECT * FROM NYCTAXI);

CREATE SAMPLE TABLE NYCTAXI_SAMPLEYEAR ON NYCTAXI_BIGDATA
  OPTIONS(buckets '7', qcs 'YEAR(TIMESTAMP(Trip_Pickup_DateTime))', fraction '0.01', strataReservoirSize '50') ;
  --AS (SELECT * FROM NYCTAXI_BIGDATA);

  CREATE SAMPLE TABLE NYCTAXI_SAMPLE_PTY ON NYCTAXI_BIGDATA
    OPTIONS(buckets '7', qcs 'payment_type', fraction '0.01', strataReservoirSize '50') ;
    --AS (SELECT * FROM NYCTAXI_BIGDATA);


