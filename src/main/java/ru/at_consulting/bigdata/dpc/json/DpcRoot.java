package ru.at_consulting.bigdata.dpc.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;
import ru.at_consulting.bigdata.dpc.json.products.ProductInfo;

/**
 * Created by NSkovpin on 22.02.2017.
 */
@Getter
@Setter
public class DpcRoot {
    @JsonIgnore
    public static final String IS_ARCHIVE = "IsArchive";
    @JsonIgnore
    public static final String DELETE = "DELETE";
    @JsonIgnore
    public static final String PUT = "PUT";

    @JsonProperty
    private String action;

    @JsonProperty
    private String timestamp;

    @JsonProperty
    private String sourceApp;

    @JsonProperty
    private String isStage;

    @JsonProperty
    private String userId;

    @JsonProperty
    private String userName;

    @JsonProperty
    private ProductInfo productInfo;

}