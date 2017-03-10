package ru.at_consulting.bigdata.dpc.json.region;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class ExternalRegionMapping {

    @JsonProperty(value = "Id")
    private String id;

    @JsonProperty(value = "SystemName")
    private String systemName;

    @JsonProperty(value = "Value")
    private String value;

}
