package ru.at_consulting.bigdata.dpc.dim.resolver;


import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;

import java.io.Serializable;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public interface DimComparator<T extends DimEntity> extends Serializable {

    Pair<T,T> resolve(DimEntity newEntity, DimEntity existingEntity);

}
