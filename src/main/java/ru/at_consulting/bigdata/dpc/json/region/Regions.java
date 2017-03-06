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
public class Regions implements IterableChildren{

    @JsonProperty(value = "Region")
    private List<Region> region;

    @Override
    public List<?> getChildren() {
        return getRegion();
    }
}
