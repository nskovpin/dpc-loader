package ru.at_consulting.bigdata.dpc.json.products;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class Products {

    @JsonProperty(value = "Product")
    private Product product;
}
