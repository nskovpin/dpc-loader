package ru.at_consulting.bigdata.dpc.dim;

import lombok.*;

import java.util.List;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Dim(name="marketingProduct")
@EqualsAndHashCode(exclude = {"effectiveDate", "expirationDate", "id"}, callSuper = false)
@Getter
@Setter
public class MarketingProductDim extends AbstractDimEntity implements Identifiable{

    private String id;

    private String title;

    private String productType;

    private List<String> productCategories;

    private String family;

    private String paymentSystem;

    private List<String> productFilters;

    private String segment;

    private String b2bSegmentGroup;

    private String equipmentType;

    @DateToString
    private String effectiveDate;

    @DateToString
    private String expirationDate;

    @Override
    public String getFirstId() {
        return getId();
    }
}
