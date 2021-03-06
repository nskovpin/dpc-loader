CREATE EXTERNAL TABLE IF NOT EXISTS ${DIGITAL}DIM_EXTERNAL_REGION_MAPPING(
	REGION_ID DOUBLE,
	ID DOUBLE,
	SYSTEM_NAME STRING,
	VALUE STRING,
	EFFECTIVE_DATE DATE)
PARTITIONED BY (EXPIRATION_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS TEXTFILE
LOCATION '${LOCATION}';