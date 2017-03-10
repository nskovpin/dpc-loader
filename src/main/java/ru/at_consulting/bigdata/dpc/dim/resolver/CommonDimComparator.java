package ru.at_consulting.bigdata.dpc.dim.resolver;

import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;

import java.io.Serializable;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class CommonDimComparator implements DimComparator<DimEntity>, Serializable {

    @Override
    public Pair<DimEntity, DimEntity> resolve(DimEntity newEntity, DimEntity existingEntity) {
        if (existingEntity == null) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);
            return Pair.of(newEntity, null);
        }

        if(newEntity == null){
            return Pair.of(null, existingEntity);
        }

        if (!newEntity.equals(existingEntity)) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);

            existingEntity.setExpirationDate(newEntity.getEffectiveDate());
            return Pair.of(newEntity, existingEntity);
        }else {
            return Pair.of(null, existingEntity);
        }
    }

}
