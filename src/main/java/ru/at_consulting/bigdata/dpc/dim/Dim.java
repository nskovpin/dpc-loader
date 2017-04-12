package ru.at_consulting.bigdata.dpc.dim;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Dim {

    String name();

    String delimiter() default "\u0001";

    String nullable() default "\\N";

    String collectionDelimiter() default "\\|";

}
