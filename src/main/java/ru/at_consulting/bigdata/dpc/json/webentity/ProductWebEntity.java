package ru.at_consulting.bigdata.dpc.json.webentity;

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
public class ProductWebEntity {

    @JsonProperty(value = "Id")
    private String id;

    @JsonProperty(value = "WebEntity")
    private WebEntity webEntity;
}
