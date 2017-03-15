package ru.at_consulting.bigdata.dpc.parser;

import org.junit.Assert;
import org.junit.Test;
import ru.at_consulting.bigdata.dpc.dim.Dim;
import ru.at_consulting.bigdata.dpc.dim.MarketingProductDim;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;

/**
 * Created by NSkovpin on 27.02.2017.
 */
public class ReflectionTest {

    @Test
    public void test(){
        Class<?> clazz = ProductDim.class;
        Dim dimMeta = clazz.getAnnotation(Dim.class);
        dimMeta.name();
        System.out.println(dimMeta.name());
    }

    @Test
    public void stringifyTest() {
        ProductDim productDim = new ProductDim();
        productDim.setId("id123");
        productDim.setIsArchive(1);
        productDim.setEffectiveDate("2999.12.12");
        String str = productDim.stringify();

        Assert.assertNotNull(str != null);
    }

    @Test
    public void toObjectTest() {
        try {
            ProductDim productDim = ProductDim.class.newInstance();

            productDim.fillObject("1\u00012\u00013\u00014\u00015\u00016\u00017");

            Assert.assertTrue(productDim.getType() != null && productDim.getId() != null);
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
        }

        MarketingProductDim marketingProductDim = new MarketingProductDim();
        marketingProductDim.fillObject("1\u00012\u00013\u00014\u00015\u00016\u00017\u00018\u00019\u000110\u00011\u00012");
        Assert.assertTrue(marketingProductDim.getId().equals("1"));
        marketingProductDim.fillObject("\\N\u0001\\N\u0001\\N\u0001\\N\u00015\u00016\u00017\u00018\u00019\u000110\u00011\u00012");
        Assert.assertTrue(marketingProductDim.getId() == null);
        marketingProductDim.fillObject("1\u00012\u00013\u00014\u00015\u00016\u00017,8,9\u00018\u00019\u000110\u00011\u00012");
        Assert.assertTrue(marketingProductDim.getProductFilters().size() == 3);
    }


}
