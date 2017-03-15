package ru.at_consulting.bigdata.dpc.dim;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 28.02.2017.
 */
@Dim(name = "productRegionLink")
@Getter
@Setter
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate"}, callSuper = false)
public class ProductRegionLinkDim extends AbstractDimEntity implements Identifiable {

    private String productId;

    private String regionId;

    @DateToString
    private String effectiveDate;

    @DateToString
    private String expirationDate;

    @Override
    public String getSecondId() {
        return getRegionId();
    }

    @Override
    public String getFirstId() {
        return getProductId();
    }
}
