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
public class WebEntity {

    @JsonProperty(value = "Id")
    private String id;

    @JsonProperty(value = "Title")
    private String title;

    @JsonProperty(value = "EntityId")
    private String entityId;

    @JsonProperty(value = "SOC")
    private String soc;

    @JsonProperty(value = "EntityType")
    private String entityType;

    @JsonProperty(value = "BusinessType")
    private String businessType;

    @JsonProperty(value = "PaySystemType")
    private String paySystemType;
}

