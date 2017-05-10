package ru.at_consulting.bigdata.dpc.dim;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

/**
 * Created by NSkovpin on 27.02.2017.
 */
public abstract class AbstractDimEntity implements DimEntity {
    public static final Logger LOGGER = Logger.getLogger(AbstractDimEntity.class);

    @Override
    public String getSecondId() {
        return null;
    }

    @Override
    public List<Field> getFields() {
        return FieldUtils.getAllFieldsList(this.getClass());
    }

    public void fillObject(String input) {
        try {
            List<Field> fieldList = getFields();

            Dim dimMeta = this.getClass().getAnnotation(Dim.class);
            if (dimMeta != null) {
                String delimiter = dimMeta.delimiter();
                String nullable = dimMeta.nullable();
                String collectionDelimiter = dimMeta.collectionDelimiter();

                String[] fieldValuesArray = input.split(delimiter, -1);
                for (int i = 0; i < fieldList.size(); i++) {
                    if (fieldList.get(i).getType() == LOGGER.getClass()) {
                        continue;
                    }
                    String value = fieldValuesArray[i];

                    Object object;
                    if (value.equals(nullable)) {
                        object = null;
                    } else if (fieldList.get(i).getType() == List.class) {
                        object = new ArrayList<>(Arrays.asList(value.split(collectionDelimiter, -1)));
                    } else {
                        object = fieldList.get(i).getType().getConstructor(String.class).newInstance(value);
                    }

                    FieldUtils.writeField(fieldList.get(i), this, object, true);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Can't create object with simple constructor:" + this.toString());
            e.printStackTrace();
            System.out.println("Input string:"+input + "; Object:"+ this.toString());
        }
    }

    public String stringify() {
        StringBuilder stringBuilder = new StringBuilder();
        Dim dimMeta = this.getClass().getAnnotation(Dim.class);
        if (dimMeta != null) {
            String delimiter = dimMeta.delimiter();
            String nullable = dimMeta.nullable();
            String collectionDelimiter = dimMeta.collectionDelimiter();

            List<Field> fieldList = getFields();
            for (Field field : fieldList) {
                if (field.getType() == LOGGER.getClass()) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object value = field.get(this);
                    appendToBuilder(stringBuilder, delimiter, collectionDelimiter, nullable, field, value);
                } catch (IllegalAccessException e) {
                    LOGGER.error("Can't get field value:" + this.toString());
                    e.printStackTrace();
                }

            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String stringifyExpirationDate() {
        String expirationDate = this.getExpirationDate();
        if (expirationDate == null) {
            return null;
        }
        Field field = FieldUtils.getField(this.getClass(), "expirationDate", true);
        if (field.getAnnotation(DateToString.class) != null) {
            String pattern = field.getAnnotation(DateToString.class).pattern();
            DateTime dateTime;
            if (expirationDate.length() == 10) {
                dateTime = DateTime.parse(expirationDate, DateTimeFormat.forPattern(pattern));
            } else {
                dateTime = DateTime.parse(expirationDate);
            }
            return dateTime.toString(pattern);
        }
        throw new RuntimeException("Class doesn't have a field");
    }

    private void appendToBuilder(StringBuilder stringBuilder, String delimiter, String collectionDelimiter, String nullable,
                                 Field field, Object value) {
        if (value == null) {
            value = nullable;

            if(field.getAnnotation(NullToString.class) != null){
                value = field.getAnnotation(NullToString.class).value();
                nullable = field.getAnnotation(NullToString.class).value();
            }
        }

        if (value instanceof List) {
            List list = (List) value;
            StringBuilder mergedValue = new StringBuilder();
            for (Object listValue : list) {
                if (mergedValue.length() == 0) {
                    mergedValue.append(listValue);
                } else {
                    mergedValue.append(collectionDelimiter.replace("\\","")).append(listValue);
                }
            }
            value = mergedValue.toString();
        }

        value = stringifyDate(field, value, nullable);

        if (stringBuilder.length() == 0) {
            stringBuilder.append(value);
        } else {
            stringBuilder.append(delimiter).append(value);
        }
    }

    private Object stringifyDate(Field field, Object value, String nullable) {
        if (value == null || value.equals(nullable)) {
            return nullable;
        }
        if (field.getAnnotation(DateToString.class) != null) {
            String pattern = field.getAnnotation(DateToString.class).pattern();
            String date = String.valueOf(value);
            DateTime dateTime;
            if (date.length() == 10) {
                dateTime = DateTime.parse(String.valueOf(value), DateTimeFormat.forPattern(pattern));
            } else {
                dateTime = DateTime.parse(String.valueOf(value));
            }
            return dateTime.toString(pattern);
        }
        return value;
    }

}
