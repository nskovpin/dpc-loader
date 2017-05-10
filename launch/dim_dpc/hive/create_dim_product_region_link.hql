CREATE EXTERNAL TABLE IF NOT EXISTS ${DIGITAL}DIM_PRODUCT_REGION_LINK(
	PRODUCT_ID DOUBLE,
	REGION_ID DOUBLE,
	EFFECTIVE_DATE DATE)
PARTITIONED BY (EXPIRATION_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION '${LOCATION}';