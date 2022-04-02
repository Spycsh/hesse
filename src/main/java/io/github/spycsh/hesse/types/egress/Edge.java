package io.github.spycsh.hesse.types.egress;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * only serve the query result
 */
public class Edge {

    @JsonProperty("src_id")
    private String srcId;

    @JsonProperty("dst_id")
    private String dstId;

    public Edge() {}

    public Edge(String srcId, String dstId) {
        this.srcId = srcId;
        this.dstId = dstId;
    }

    public String getSrcId() {
        return srcId;
    }

    public String getDstId() {
        return dstId;
    }

}
