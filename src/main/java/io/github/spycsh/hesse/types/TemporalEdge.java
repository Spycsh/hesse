package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TemporalEdge {
    @JsonProperty("src_id")
    private String srcId;

    @JsonProperty("dst_id")
    private String dstId;

    @JsonProperty("timestamp")
    private String timestamp;

    public TemporalEdge() {}

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

    public String getTimestamp() {
        return timestamp;
    }
}
