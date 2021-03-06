package ru.at_consulting.bigdata.dpc.json.region;

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
public class Region {

    @JsonProperty(value = "Id")
    private String id;

    @JsonProperty(value = "Title")
    private String title;

    @JsonProperty(value = "ExternalRegionMappings")
    private ExternalRegionMappings externalRegionMappings;

}
