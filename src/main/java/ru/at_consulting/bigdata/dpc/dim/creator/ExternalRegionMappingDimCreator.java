package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.ExternalRegionMappingDim;
import ru.at_consulting.bigdata.dpc.dim.WebEntityDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.region.ExternalRegionMapping;
import ru.at_consulting.bigdata.dpc.json.region.Region;
import ru.at_consulting.bigdata.dpc.json.webentity.WebEntity;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class ExternalRegionMappingDimCreator implements DimCreator<ExternalRegionMappingDim, ExternalRegionMapping> {

    @Override
    public <HOLDER extends IterableChildren> List<ExternalRegionMappingDim> create(DpcRoot dpcRoot, HOLDER holder) {
        List<ExternalRegionMappingDim> webEntityDimList = new ArrayList<>();
        List<?> children = holder.getChildren();
        for(Object child: children){
            if(child instanceof ExternalRegionMapping){
                webEntityDimList.add(create(dpcRoot, (ExternalRegionMapping) child));
            }
        }
        return webEntityDimList;
    }

    @Override
    public ExternalRegionMappingDim create(DpcRoot dpcRoot, ExternalRegionMapping parent) {
        ExternalRegionMappingDim externalRegionMappingDim = new ExternalRegionMappingDim();
        externalRegionMappingDim.setId(parent.getId());
        externalRegionMappingDim.setSystemName(parent.getSystemName());
        externalRegionMappingDim.setValue(parent.getValue());
        externalRegionMappingDim.setEffectiveDate(dpcRoot.getTimestamp());
        return externalRegionMappingDim;
    }

    public List<ExternalRegionMappingDim> create(DpcRoot dpcRoot, Region region){
        List<ExternalRegionMappingDim> externalRegionMappingDimList = new ArrayList<>();
        for(ExternalRegionMapping externalRegionMapping :region.getExternalRegionMappings().getExternalRegionMapping()){
            create(dpcRoot, region, externalRegionMapping);
        }
        return externalRegionMappingDimList;
    }

    public ExternalRegionMappingDim create(DpcRoot dpcRoot, Region region, ExternalRegionMapping parent){
        ExternalRegionMappingDim externalRegionMappingDim = create(dpcRoot, parent);
        externalRegionMappingDim.setId(region.getId());
        return  externalRegionMappingDim;
    }

}
