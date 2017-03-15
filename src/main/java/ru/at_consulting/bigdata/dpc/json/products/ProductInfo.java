package ru.at_consulting.bigdata.dpc.json.products;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 26.02.2017.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductInfo {

    @JsonProperty(value = "Products")
    private Products products;
}
