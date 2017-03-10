package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.ProductDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.modifiers.ProductModifier;
import ru.at_consulting.bigdata.dpc.json.products.Product;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class ProductDimCreator implements DimCreator<ProductDim, Product> {

    @Override
    public <HOLDER extends IterableChildren> List<ProductDim> create(DpcRoot dpcRoot, HOLDER holder) {
//        List<ProductDim> productDims = new ArrayList<>();
//        List<Product> products = null;
//        for (Product product: products) {
//            ProductDim productDim = new ProductDim();
//            productDim.setId(product.getId());
//            productDim.setType(product.getType());
//            productDim.setMarketingProductId(product.getMarketingProduct().getId());
//            Integer isArchive = 0;
//            for (ProductModifier modifier : product.getModifiers().getProductModifier()) {
//                isArchive = modifier.getAlias().equals(DpcRoot.IS_ARCHIVE) ? 1 : 0;
//            }
//            productDim.setIsArchive(isArchive);
//            productDim.setIsDel(dpcRoot.getAction().equals(DpcRoot.DELETE) ? 1 : 0);
//            productDim.setEffectiveDate(dpcRoot.getTimestamp());
//
//            productDims.add(productDim);
//        }
        return null;
    }

    @Override
    public ProductDim create(DpcRoot dpcRoot, Product parent) {
        ProductDim productDim = new ProductDim();
        productDim.setId(parent.getId());
        productDim.setType(parent.getType());
        if (parent.getMarketingProduct() != null) {
            productDim.setMarketingProductId(parent.getMarketingProduct().getId());
        }
        Integer isArchive = 0;
        if(parent.getModifiers() != null){
            for (ProductModifier modifier : parent.getModifiers().getProductModifier()) {
                if (modifier.getAlias() != null) {
                    isArchive = modifier.getAlias().equals(DpcRoot.IS_ARCHIVE) ? 1 : 0;
                }
            }
        }
        productDim.setIsArchive(isArchive);
        productDim.setIsDel(dpcRoot.getAction().equals(DpcRoot.DELETE) ? 1 : 0);
        productDim.setEffectiveDate(dpcRoot.getTimestamp());
        return productDim;
    }

}
