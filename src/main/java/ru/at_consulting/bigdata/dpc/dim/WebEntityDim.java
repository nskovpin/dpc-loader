package ru.at_consulting.bigdata.dpc.dim;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 28.02.2017.
 */
@Dim(name = "webEntity")
@Getter
@Setter
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "entityId", "productId"}, callSuper = false)
public class WebEntityDim extends AbstractDimEntity implements Identifiable{

    private String entityId;

    private String productId;

    private String soc;

    private String entityType;

    private String businessType;

    private String paySystemType;

    private String title;

    private String effectiveDate;

    private String expirationDate;

    @Override
    public String getFirstId() {
        return null;
    }
}
