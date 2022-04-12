package io.github.spycsh.hesse.types.ingress;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class TemporalEdge {
    @JsonProperty("src_id")
    private String srcId;

    @JsonProperty("dst_id")
    private String dstId;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty("weight")
    private String weight;

    @JsonProperty("timestamp")
    private String timestamp;


    public TemporalEdge() {}

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public String getWeight() {
        return weight;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
