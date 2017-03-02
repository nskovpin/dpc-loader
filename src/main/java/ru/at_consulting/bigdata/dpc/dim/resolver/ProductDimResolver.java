package ru.at_consulting.bigdata.dpc.dim.resolver;

import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class ProductDimResolver implements DimResolver<ProductDim> {

    @Override
    public Pair<ProductDim, ProductDim> resolve(ProductDim newEntity, ProductDim existingEntity) {
        if (existingEntity == null) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);
            return Pair.of(newEntity, null);
        }

        if (!newEntity.equals(existingEntity)) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);

            if (newEntity.getIsDel() != null && newEntity.getIsDel() == 1) {
                newEntity.setType(existingEntity.getType());
                newEntity.setMarketingProductId(existingEntity.getMarketingProductId());
                newEntity.setIsArchive(existingEntity.getIsArchive());
            }

            existingEntity.setExpirationDate(newEntity.getEffectiveDate());
            return Pair.of(newEntity, existingEntity);
        } else {
            return Pair.of(null, existingEntity);
        }
    }


}
