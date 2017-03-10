package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 02.03.2017.
 */
@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductFilter {

    @JsonProperty(value = "Title")
    private String title;

}
