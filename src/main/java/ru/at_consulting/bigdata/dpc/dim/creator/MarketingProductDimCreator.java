package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.MarketingProductDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.marketing.MarketingProduct;
import ru.at_consulting.bigdata.dpc.json.marketing.ProductCategory;
import ru.at_consulting.bigdata.dpc.json.marketing.ProductFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class MarketingProductDimCreator implements DimCreator<MarketingProductDim, MarketingProduct> {


    @Override
    public <HOLDER extends IterableChildren> List<MarketingProductDim> create(DpcRoot dpcRoot, HOLDER holder) {
        return null;
    }

    @Override
    public MarketingProductDim create(DpcRoot dpcRoot, MarketingProduct parent) {
        MarketingProductDim marketingProductDim = new MarketingProductDim();
        marketingProductDim.setId(parent.getId());
        marketingProductDim.setTitle(parent.getTitle());
        marketingProductDim.setProductType(parent.getProductType());

        List<String> productCategoriesTitles = null;
        if(parent.getCategories() != null){
            productCategoriesTitles = new ArrayList<>();
            for(ProductCategory productCategory : parent.getCategories().getProductCategory()){
                productCategoriesTitles.add(productCategory.getTitle());
            }
        }
        marketingProductDim.setProductCategories(productCategoriesTitles);

        if(parent.getFamily() != null){
            marketingProductDim.setFamily(parent.getFamily().getTitle());
        }
        marketingProductDim.setPaymentSystem(parent.getPaymentSystem());

        List<String> productFilterTitles = null;
        if(parent.getProductFilters() != null){
            productFilterTitles = new ArrayList<>();
            for(ProductFilter productFilter : parent.getProductFilters().getProductFilter()){
                productFilterTitles.add(productFilter.getTitle());
            }
        }
        marketingProductDim.setProductFilters(productFilterTitles);

        if(parent.getSegment() != null){
            marketingProductDim.setSegment(parent.getSegment().getTitle());

            if(parent.getSegment().getGroups() != null && parent.getSegment().getGroups().getB2BSegmentGroup() != null){
                marketingProductDim.setB2bSegmentGroup(parent.getSegment().getGroups().getB2BSegmentGroup().getTitle());
            }
        }

        if(parent.getEquipmentType() != null){
            marketingProductDim.setEquipmentType(parent.getEquipmentType().getTitle());
        }
        marketingProductDim.setEffectiveDate(dpcRoot.getTimestamp());

        return marketingProductDim;
    }


}
