package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketingProduct {

    @JsonProperty(value = "Id")
    private String id;

    @JsonProperty(value = "Title")
    private String title;

    @JsonProperty(value = "ProductType")
    private String productType;

    @JsonProperty(value = "PaymentSystem")
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
