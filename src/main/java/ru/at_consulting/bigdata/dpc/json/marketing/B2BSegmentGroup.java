package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 02.03.2017.
 */
@Getter
@Setter
public class B2BSegmentGroup {

    @JsonProperty
    private String title;
}
