package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.ProductRegionLinkDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.region.Region;
import ru.at_consulting.bigdata.dpc.json.region.Regions;

import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class ProductRegionLinkDimCreator implements DimCreator<ProductRegionLinkDim, Region> {

    @Override
    public <HOLDER extends IterableChildren> List<ProductRegionLinkDim> create(DpcRoot dpcRoot, HOLDER holder) {
        return null;
    }

    @Override
    public ProductRegionLinkDim create(DpcRoot dpcRoot, Region parent) {
        ProductRegionLinkDim productRegionLinkDim = new ProductRegionLinkDim();
        productRegionLinkDim.setProductId(dpcRoot.getProductInfo().getProducts().getProduct().getId());
        productRegionLinkDim.setRegionId(parent.getId());
        productRegionLinkDim.setEffectiveDate(dpcRoot.getTimestamp());
        return productRegionLinkDim;
    }
}
