package ru.at_consulting.bigdata.dpc.dim.resolver;

import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class CommonDimResolver implements DimResolver<DimEntity> {

    @Override
    public Pair<DimEntity, DimEntity> resolve(DimEntity newEntity, DimEntity existingEntity) {
        if (existingEntity == null) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);
            return Pair.of(newEntity, null);
        }

        if (!newEntity.equals(existingEntity)) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);

            existingEntity.setExpirationDate(newEntity.getEffectiveDate());
            return Pair.of(newEntity, existingEntity);
        }

        return null;
    }

}
