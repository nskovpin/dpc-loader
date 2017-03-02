package ru.at_consulting.bigdata.dpc.json.webentity;

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
public class ProductWebEntities implements IterableChildren{

    @JsonProperty
    private List<ProductWebEntity> productWebEntity;

    @Override
    public List<?> getChildren() {
        return getProductWebEntity();
    }

}
