ALTER TABLE ${SPP}STG_TAXI_USER_ACTIVITY
ADD IF NOT EXISTS PARTITION(TIME_KEY='${TIME_KEY}')
LOCATION '${INPUT_DIR}';