package io.github.spycsh.hesse.types.ingress;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * deprecated
 */
public class TemporalWeightedEdge {
    @JsonProperty("src_id")
    private String srcId;

    @JsonProperty("dst_id")
    private String dstId;

    @JsonProperty("weight")
    private String weight;

    @JsonProperty("timestamp")
    private String timestamp;

    public TemporalWeightedEdge() {}

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public String getWeight() {return weight; }

    public String getTimestamp() {
        return timestamp;
    }

}
