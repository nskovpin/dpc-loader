CREATE EXTERNAL TABLE IF NOT EXISTS ${DIGITAL}DIM_PRODUCT_MAP(
	PRODUCT_ID DOUBLE,
	REGION_ID DOUBLE,
	EXTERNAL_REGION_ID DOUBLE,
	EXTERNAL_SYSTEM_NAME STRING,
	EXTERNAL_REGION_VALUE STRING,
	ENTITY_ID STRING,
	SOC STRING,
	ENTITY_TYPE STRING,
	PAY_SYSTEM_TYPE STRING,
	EFFECTIVE_DATE DATE)
PARTITIONED BY (EXPIRATION_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION '${LOCATION}';