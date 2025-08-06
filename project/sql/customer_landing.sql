CREATE EXTERNAL TABLE IF NOT EXISTS customer_landing (
  customerName STRING,
  email STRING,
  phone STRING,
  birthDay STRING,
  serialNumber STRING,
  registrationDate BIGINT,
  lastUpdateDate BIGINT,
  shareWithResearchAsOfDate BIGINT,
  shareWithPublicAsOfDate BIGINT,
  shareWithFriendsAsOfDate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
)
LOCATION 's3://customer-landing-wgu/'
TBLPROPERTIES ('has_encrypted_data'='false');

  
  select * from customer_landing.customer_landing limit 10
