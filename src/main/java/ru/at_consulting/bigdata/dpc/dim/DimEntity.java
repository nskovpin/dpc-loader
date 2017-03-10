package ru.at_consulting.bigdata.dpc.dim;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.util.List;

/**
 * Created by NSkovpin on 27.02.2017.
 */
public interface DimEntity extends Serializable, Identifiable{

    String EXPIRATION_DATE_INFINITY = "2999.12.31";

    List<Field> getFields();

    void fillObject(String input);

    String stringify();

    void setEffectiveDate(String date);

    void setExpirationDate(String date);

    String getEffectiveDate();

    String getExpirationDate();

}
