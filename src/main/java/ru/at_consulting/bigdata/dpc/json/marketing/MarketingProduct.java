package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class MarketingProduct {

    @JsonProperty
    private String id;

    @JsonProperty
    private String title;

    @JsonProperty
    private String productType;

    @JsonProperty
    private String paymentSystem;

    @JsonProperty(value = "Categories")
    private Categories categories;

    @JsonProperty(value = "Family")
    private Family family;

    @JsonProperty(value = "ProductFilters")
    private ProductFilters productFilters;

    @JsonProperty(value = "Segment")
    private Segment segment;

    @JsonProperty(value = "EquipmentType")
    private EquipmentType equipmentType;
}
