package ru.at_consulting.bigdata.dpc.cluster.loader;

import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.dim.ProductDim;
import ru.at_consulting.bigdata.dpc.json.modifiers.ProductModifier;
import ru.at_consulting.bigdata.dpc.json.products.Product;

/**
 * Created by NSkovpin on 27.02.2017.
 */
public class ProductResolver implements DimResolver<Product, ProductDim>{

    public ProductDim resolve(Product product, DpcRoot dpcRoot){
        ProductDim productDim = new ProductDim();
        productDim.setId(product.getId());
        productDim.setType(product.getType());
        productDim.setMarketingProductId(product.getMarketingProduct().getId());
        Integer isArchive = 0;
        for (ProductModifier modifier : product.getModifiers().getProductModifier()) {
            isArchive = modifier.getAlias().equals(DpcRoot.IS_ARCHIVE) ? 1 : 0;
        }
        productDim.setIsArchive(isArchive);
        productDim.setIsDel(dpcRoot.getAction().equals(DpcRoot.DELETE) ? 1 : 0);
        return null;
    }

}
