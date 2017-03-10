package ru.at_consulting.bigdata.dpc.dim.resolver;

import ru.at_consulting.bigdata.dpc.dim.DimEntity;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;

import java.io.Serializable;

/**
 * Created by NSkovpin on 09.03.2017.
 */
public class DimComparatorFactory implements Serializable{

    private ProductDimComparator productDimComparator;

    private CommonDimComparator commonDimComparator;

    public DimComparatorFactory() {
        this.productDimComparator = new ProductDimComparator();
        this.commonDimComparator = new CommonDimComparator();
    }

    public <T extends DimEntity> DimComparator getComparator(Class<T> clazzDim){
        if(clazzDim == ProductDim.class){
            return productDimComparator;
        }else{
            return commonDimComparator;
        }
    }

}
