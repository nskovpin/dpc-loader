package ru.at_consulting.bigdata.dpc.dim;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 28.02.2017.
 */
@Dim(name = "region")
@Getter
@Setter
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "id"}, callSuper = false)
public class RegionDim extends AbstractDimEntity implements Identifiable {

    private String id;

    private String title;

    private String effectiveDate;

    private String expirationDate;

    @Override
    public String getFirstId() {
        return getId();
    }
}
