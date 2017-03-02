package ru.at_consulting.bigdata.dpc.dim.creator;

import ru.at_consulting.bigdata.dpc.dim.RegionDim;
import ru.at_consulting.bigdata.dpc.json.DpcRoot;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;
import ru.at_consulting.bigdata.dpc.json.region.Region;

import java.util.List;

/**
 * Created by NSkovpin on 02.03.2017.
 */
public class RegionDimCreator implements DimCreator<RegionDim, Region> {

    @Override
    public <HOLDER extends IterableChildren> List<RegionDim> create(DpcRoot dpcRoot, HOLDER holder) {
        return null;
    }

    @Override
    public RegionDim create(DpcRoot dpcRoot, Region parent) {
        RegionDim regionDim = new RegionDim();
        regionDim.setId(parent.getId());
        regionDim.setTitle(parent.getTitle());
        regionDim.setEffectiveDate(dpcRoot.getTimestamp());
        return regionDim;
    }
}
