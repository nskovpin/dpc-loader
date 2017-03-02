package ru.at_consulting.bigdata.dpc.json.webentity;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class ProductWebEntity {

    @JsonProperty
    private WebEntity webEntity;
}
