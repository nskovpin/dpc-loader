package ru.at_consulting.bigdata.dpc.dim;

import lombok.*;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Dim(name = "product")
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "id"}, callSuper = false)
@Getter
@Setter
public class ProductDim extends AbstractDimEntity implements Identifiable{

    private String id;

    private String type;

    @NullToString("-99")
    private String marketingProductId;

    private Integer isArchive;

    private Integer isDel;

    @DateToString
    private String effectiveDate;

    @DateToString
    private String expirationDate;

    @Override
    public String getFirstId() {
        return getId();
    }
}
