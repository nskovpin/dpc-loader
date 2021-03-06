package ru.at_consulting.bigdata.dpc.dim;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 28.02.2017.
 */
@Dim(name = "externalRegionMapping")
@Getter
@Setter
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "regionId", "id"}, callSuper = false)
public class ExternalRegionMappingDim extends AbstractDimEntity implements Identifiable {

    private String regionId;

    private String id;

    private String systemName;

    private String value;

    @DateToString
    private String effectiveDate;

    @DateToString
    private String expirationDate;

    @Override
    public String getFirstId() {
        return regionId;
    }

    @Override
    public String getSecondId() {
        return id;
    }
}
