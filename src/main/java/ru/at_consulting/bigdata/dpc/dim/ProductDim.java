package ru.at_consulting.bigdata.dpc.dim;

import lombok.*;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Dim(name = "product")
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "id"}, callSuper = false)
@Getter
@Setter
public class ProductDim extends AbstractDimEntity {

    private String id;

    private String type;

    private String marketingProductId;

    private Integer isArchive;

    private Integer isDel;

    private String effectiveDate;

    private String expirationDate;

}
