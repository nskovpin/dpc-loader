package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class DimCreatorFactory implements Serializable{

    private static final Map<Class, DimCreator> creatorMap;

    static {
        creatorMap = new HashMap<>();
        creatorMap.put(ProductDim.class, new ProductDimCreator());
        creatorMap.put(ExternalRegionMappingDim.class, new ExternalRegionMappingDimCreator());
        creatorMap.put(MarketingProductDim.class, new MarketingProductDimCreator());
        creatorMap.put(ProductRegionLinkDim.class, new ProductRegionLinkDimCreator());
        creatorMap.put(RegionDim.class, new RegionDimCreator());
        creatorMap.put(WebEntityDim.class, new WebEntityDimCreator());
        creatorMap.put(ProductMapDim.class, new ProductMapDimCreator());
    }

    @SuppressWarnings("unchecked")
    public static <T extends DimEntity, P> DimCreator<T, P> getCreator(Class<T> dimClass){
        return creatorMap.get(dimClass);
    }

    public static ProductMapDimCreator getProductMapDimCreator(){
        return (ProductMapDimCreator) creatorMap.get(ProductMapDim.class);
    }

}
