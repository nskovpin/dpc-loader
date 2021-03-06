package ru.at_consulting.bigdata.dpc.dim;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by NSkovpin on 13.03.2017.
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface DateToString {

    String pattern() default "YYYY-MM-dd";

}
