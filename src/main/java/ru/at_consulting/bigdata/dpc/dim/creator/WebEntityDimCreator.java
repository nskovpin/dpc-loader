package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.WebEntityDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.webentity.WebEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class WebEntityDimCreator implements DimCreator<WebEntityDim, WebEntity> {

    @Override
    public <HOLDER extends IterableChildren> List<WebEntityDim> create(DpcRoot dpcRoot, HOLDER holder) {
        List<WebEntityDim> webEntityDimList = new ArrayList<>();
        List<?> children = holder.getChildren();
        for(Object child: children){
            if(child instanceof WebEntity){
                webEntityDimList.add(create(dpcRoot, (WebEntity) child));
            }
        }
        return webEntityDimList;
    }

    @Override
    public WebEntityDim create(DpcRoot dpcRoot, WebEntity parent) {
        WebEntityDim webEntityDim = new WebEntityDim();
        webEntityDim.setEntityId(parent.getEntityId());
        webEntityDim.setProductId(dpcRoot.getProductInfo().getProducts().getProduct().getId());
        webEntityDim.setSoc(parent.getSoc());
        webEntityDim.setEntityType(parent.getEntityType());
        webEntityDim.setBusinessType(parent.getBusinessType());
        webEntityDim.setPaySystemType(parent.getPaySystemType());
        webEntityDim.setTitle(parent.getTitle());
        webEntityDim.setEffectiveDate(dpcRoot.getTimestamp());
        return webEntityDim;
    }
}
