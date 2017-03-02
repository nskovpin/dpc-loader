package ru.at_consulting.bigdata.dpc.json.region;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import ru.at_consulting.bigdata.dpc.json.IterableChildren;

import java.util.List;

/**
 * Created by NSkovpin on 27.02.2017.
 */
@Getter
@Setter
public class ExternalRegionMappings implements IterableChildren {

    @JsonProperty
    private List<ExternalRegionMapping> externalRegionMapping;

    @Override
    public List<?> getChildren() {
        return getExternalRegionMapping();
    }
}
