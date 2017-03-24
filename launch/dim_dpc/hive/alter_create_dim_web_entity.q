ALTER TABLE ${DIGITAL}DIM_WEB_ENTITY
ADD IF NOT EXISTS PARTITION(TIME_KEY='${TIME_KEY}')
LOCATION '${INPUT_DIR}';
ALTER TABLE ${DIGITAL}DIM_WEB_ENTITY
ADD IF NOT EXISTS PARTITION(TIME_KEY='${DATE_OPENED}')
LOCATION '${INPUT_OPENED_DIR}';