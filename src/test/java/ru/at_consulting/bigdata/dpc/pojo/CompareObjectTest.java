package ru.at_consulting.bigdata.dpc.pojo;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;
import ru.at_consulting.bigdata.dpc.dim.resolver.ProductDimComparator;

/**
 * Created by NSkovpin on 01.03.2017.
 */
public class CompareObjectTest {

    @Test
    public void compareTwoDimObjects(){
        ProductDim productNewDim = new ProductDim();
        productNewDim.setType("type");
        productNewDim.setMarketingProductId("id");
        productNewDim.setId("1");

        ProductDim productExistingDim = new ProductDim();
        productExistingDim.setType("type");
        productExistingDim.setMarketingProductId("id");
        productExistingDim.setId("2");

        ProductDimComparator productDimResolver = new ProductDimComparator();
        Pair<ProductDim, ProductDim> products = productDimResolver.resolve(productNewDim, productExistingDim);

        ProductDim productExistingDim2 = new ProductDim();
        productExistingDim2.setType("type1");
        productExistingDim2.setMarketingProductId("id");

        Pair<ProductDim, ProductDim> products2 = productDimResolver.resolve(productNewDim, productExistingDim2);
        Assert.assertTrue(products2.getLeft() != null && products2.getRight() != null);
    }

}
