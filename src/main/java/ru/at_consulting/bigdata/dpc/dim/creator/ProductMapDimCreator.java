package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.ExternalRegionMappingDim;
import ru.at_consulting.bigdata.dpc.dim.ProductMapDim;
import ru.at_consulting.bigdata.dpc.dim.WebEntityDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.region.Region;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 11.04.2017.
 */
public class ProductMapDimCreator implements DimCreator<ProductMapDim, Region> {

    @Override
    public <HOLDER extends IterableChildren> List<ProductMapDim> create(DpcRoot dpcRoot, HOLDER holder) {
        throw new RuntimeException("Not supported"); // because of many-to-many
    }

    @Override
    public ProductMapDim create(DpcRoot dpcRoot, Region region) {
        throw new RuntimeException("Not supported"); // because of many-to-many
    }

    public List<ProductMapDim> create(DpcRoot dpcRoot, Region region, List<WebEntityDim> webEntityDims, List<ExternalRegionMappingDim> externalRegionMappingDims) {
        List<ProductMapDim> productMapDimList = new ArrayList<>();
        if (webEntityDims != null && webEntityDims.size() > 0) {
            for (WebEntityDim webEntityDim : webEntityDims) {

                if (externalRegionMappingDims != null && externalRegionMappingDims.size() > 0) {
                    for (ExternalRegionMappingDim externalRegionMappingDim : externalRegionMappingDims) {
                        ProductMapDim productMapDimNew = copyAndSetWeb(dpcRoot, region, webEntityDim);
                        productMapDimNew.setExternalRegionId(externalRegionMappingDim.getId());
                        productMapDimNew.setExternalSystemName(externalRegionMappingDim.getSystemName());
                        productMapDimNew.setExternalRegionValue(externalRegionMappingDim.getValue());

                        productMapDimList.add(productMapDimNew);
                    }
                } else {
                    productMapDimList.add(copyAndSetWeb(dpcRoot, region, webEntityDim));
                }
            }

        } else if (externalRegionMappingDims != null && externalRegionMappingDims.size() > 0) {
            for (ExternalRegionMappingDim externalRegionMappingDim : externalRegionMappingDims) {
                ProductMapDim productMapDimNew = copyAndSetExternal(dpcRoot, region, externalRegionMappingDim);
                productMapDimList.add(productMapDimNew);
            }
        } else {
            productMapDimList.add(copy(dpcRoot, region));
        }
        return productMapDimList;
    }

    private ProductMapDim copyAndSetWeb(DpcRoot dpcRoot, Region region, WebEntityDim webEntityDim) {
        ProductMapDim productMapDimNew = copy(dpcRoot, region);
        productMapDimNew.setEntityType(webEntityDim.getEntityType());
        productMapDimNew.setSoc(webEntityDim.getSoc());
        productMapDimNew.setEntityId(webEntityDim.getEntityId());
        productMapDimNew.setPaySystemType(webEntityDim.getPaySystemType());
        return productMapDimNew;
    }

    private ProductMapDim copyAndSetExternal(DpcRoot dpcRoot, Region region, ExternalRegionMappingDim externalRegionMappingDim) {
        ProductMapDim productMapDimNew = copy(dpcRoot, region);
        productMapDimNew.setExternalRegionId(externalRegionMappingDim.getId());
        productMapDimNew.setExternalSystemName(externalRegionMappingDim.getSystemName());
        productMapDimNew.setExternalRegionValue(externalRegionMappingDim.getValue());
        return productMapDimNew;
    }

    private ProductMapDim copy(DpcRoot dpcRoot, Region region) {
        ProductMapDim productMapDimNew = new ProductMapDim();
        productMapDimNew.setProductId(dpcRoot.getProductInfo().getProducts().getProduct().getId());
        if (region != null) {
            productMapDimNew.setRegionId(region.getId());
        }
        productMapDimNew.setEffectiveDate(dpcRoot.getTimestamp());
        return productMapDimNew;
    }

}
