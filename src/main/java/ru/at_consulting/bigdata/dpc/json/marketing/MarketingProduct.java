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

    @JsonProperty
    private Categories categories;

    @JsonProperty
    private Family family;

    @JsonProperty
    private ProductFilters productFilters;

    @JsonProperty
    private Segment segment;

    @JsonProperty
    private EquipmentType equipmentType;
}
