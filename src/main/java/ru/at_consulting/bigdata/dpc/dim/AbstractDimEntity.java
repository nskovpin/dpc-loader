package ru.at_consulting.bigdata.dpc.dim;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.log4j.Logger;

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

    public void fillObject(String input){
        try {
            List<Field> fieldList = getFields();

            Dim dimMeta = this.getClass().getAnnotation(Dim.class);
            if (dimMeta != null) {
                String delimiter = dimMeta.delimiter();
                String nullable = dimMeta.nullable();

                String[] fieldValuesArray = input.split(delimiter, -1);
                for (int i = 0; i < fieldList.size(); i ++) {
                    if(fieldList.get(i).getType() == LOGGER.getClass()){
                        continue;
                    }
                    String value =  fieldValuesArray[i];

                    Object object;
                    if(value.equals(nullable)){
                        object = null;
                    }else if(fieldList.get(i).getType() == List.class){
                        object = new ArrayList<>(Arrays.asList(value.split(",",-1)));
                    }else{
                        object = fieldList.get(i).getType().getConstructor(String.class).newInstance(value);
                    }

                    FieldUtils.writeField(fieldList.get(i), this, object, true);
                }
            }
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
            LOGGER.error("Can't create object with simple constructor:" + this.toString());
            e.printStackTrace();
        }
    }

    public String stringify() {
        StringBuilder stringBuilder = new StringBuilder();
        Dim dimMeta = this.getClass().getAnnotation(Dim.class);
        if (dimMeta != null) {
            String delimiter = dimMeta.delimiter();
            String nullable = dimMeta.nullable();

            List<Field> fieldList = getFields();
            for(Field field: fieldList){
                if(field.getType() == LOGGER.getClass()){
                    continue;
                }
                field.setAccessible(true);
                try {
                    Object value = field.get(this);
                    appendToBuilder(stringBuilder, delimiter, nullable, value);
                } catch (IllegalAccessException e) {
                    LOGGER.error("Can't get field value:" + this.toString());
                    e.printStackTrace();
                }

            }
        }
        return stringBuilder.toString();
    }

    private void appendToBuilder(StringBuilder stringBuilder, String delimiter, String nullable, Object value){
        if(value == null){
            value = nullable;
        }

        if(value instanceof List){
            List list = (List) value;
            StringBuilder mergedValue =  new StringBuilder();
            for (Object listValue: list) {
                if(mergedValue.length() == 0){
                    mergedValue.append(listValue);
                }else {
                    mergedValue.append(",").append(listValue);
                }
            }
            value = mergedValue.toString();
        }

        if(stringBuilder.length() == 0){
            stringBuilder.append(value);
        }else{
            stringBuilder.append(delimiter).append(value);
        }
    }

}
