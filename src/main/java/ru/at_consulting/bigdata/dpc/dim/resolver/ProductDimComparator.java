package ru.at_consulting.bigdata.dpc.dim.resolver;

import org.apache.commons.lang3.tuple.Pair;
import ru.at_consulting.bigdata.dpc.dim.DimEntity;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;

import java.io.Serializable;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class ProductDimComparator implements DimComparator<ProductDim>, Serializable {

    @Override
    public Pair<ProductDim, ProductDim> resolve(DimEntity newEntity, DimEntity existingEntity) {
        ProductDim newEntityProduct = (ProductDim) newEntity;
        ProductDim existingEntityProduct = (ProductDim) existingEntity;

        if (existingEntity == null) {
            newEntityProduct.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);
            return Pair.of(newEntityProduct, null);
        }

        if(newEntity == null){
            return Pair.of(null, existingEntityProduct);
        }

        if (!newEntity.equals(existingEntity)) {
            newEntity.setExpirationDate(DimEntity.EXPIRATION_DATE_INFINITY);

            if (newEntityProduct.getIsDel() != null && newEntityProduct.getIsDel() == 1) {
                newEntityProduct.setType(existingEntityProduct.getType());
                newEntityProduct.setMarketingProductId(existingEntityProduct.getMarketingProductId());
                newEntityProduct.setIsArchive(existingEntityProduct.getIsArchive());
            }

            existingEntity.setExpirationDate(newEntityProduct.getEffectiveDate());
            return Pair.of(newEntityProduct, existingEntityProduct);
        } else {
            return Pair.of(null, existingEntityProduct);
        }
    }


}
