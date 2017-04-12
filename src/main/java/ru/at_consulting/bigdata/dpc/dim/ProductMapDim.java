package ru.at_consulting.bigdata.dpc.dim;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by NSkovpin on 11.04.2017.
 */
@Dim(name = "productMap", nullable = "-99")
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "productId", "regionId", "externalRegionId", "entityId"}, callSuper = false)
@Getter
@Setter
public class ProductMapDim extends AbstractDimEntity implements Identifiable {

    private String productId;

    private String regionId;

    private String externalRegionId;

    private String externalSystemName;

    private String externalRegionValue;

    private String entityId;

    private String soc;

    private String entityType;

    private String paySystemType;

    @DateToString
    private String effectiveDate;

    @DateToString
    private String expirationDate;

    @Override
    public String getFirstId() {
        return getProductId();
    }

    @Override
    public String getSecondId() {
        return regionId + ";" + externalRegionId + ";" + entityId;
    }

}
