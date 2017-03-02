package ru.at_consulting.bigdata.dpc.dim.resolver;


import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public interface DimResolver<T extends DimEntity> {

    Pair<T,T> resolve(T newEntity, T existingEntity);

}
