CREATE EXTERNAL TABLE IF NOT EXISTS ${DIGITAL}DIM_MARKETING_PRODUCT(
	ID DOUBLE,
	TITLE STRING,
	PRODUCT_TYPE STRING,
	PRODUCT_CATEGORY ARRAY<STRING>,
	FAMILY STRING,
	PAYMENT_SYSTEM STRING,
	PRODUCT_FILTER ARRAY<STRING>,
	SEGMENT STRING,
	B2B_SEGMENT_GROUP STRING,
	EQUIPMENT_TYPE STRING,
	EFFECTIVE_DATE DATE)
PARTITIONED BY (EXPIRATION_DATE DATE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001' COLLECTION ITEMS TERMINATED BY '\|'
STORED AS TEXTFILE
LOCATION '${LOCATION}';