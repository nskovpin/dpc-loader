package ru.at_consulting.bigdata.dpc.json.webentity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class WebEntity {

    @JsonProperty
    private String id;

    @JsonProperty
    private String title;

    @JsonProperty
    private String entityId;

    @JsonProperty
    private String soc;

    @JsonProperty
    private String entityType;

    @JsonProperty
    private String businessType;

    @JsonProperty
    private String paySystemType;
}

