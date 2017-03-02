package ru.at_consulting.bigdata.dpc.json.modifiers;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class Modifiers {

    @JsonProperty
    private List<ProductModifier> productModifier;
}
