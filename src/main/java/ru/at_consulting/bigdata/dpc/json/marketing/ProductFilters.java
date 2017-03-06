package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class ProductFilters {

    @JsonProperty(value = "ProductFilter")
    private List<ProductFilter> productFilter;
}
