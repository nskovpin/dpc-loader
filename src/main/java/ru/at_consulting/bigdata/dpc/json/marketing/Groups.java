package ru.at_consulting.bigdata.dpc.json.marketing;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Created by NSkovpin on 02.03.2017.
 */
@Getter
@Setter
public class Groups {

    @JsonProperty(value = "B2BSegmentGroup")
    private B2BSegmentGroup b2BSegmentGroup;

}
