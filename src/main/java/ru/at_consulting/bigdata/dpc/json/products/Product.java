package ru.at_consulting.bigdata.dpc.json.products;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import ru.at_consulting.bigdata.dpc.json.marketing.MarketingProduct;
import ru.at_consulting.bigdata.dpc.json.modifiers.Modifiers;
import ru.at_consulting.bigdata.dpc.json.region.Regions;
import ru.at_consulting.bigdata.dpc.json.webentity.ProductWebEntities;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class Product {

    @JsonProperty
    private String id;

    @JsonProperty
    private String type;

    @JsonProperty(value = "MarketingProduct")
    private MarketingProduct marketingProduct;

    @JsonProperty(value = "Regions")
    private Regions regions;

    @JsonProperty(value = "Modifiers")
    private Modifiers modifiers;

    @JsonProperty(value = "ProductWebEntities")
    private ProductWebEntities productWebEntities;

}
