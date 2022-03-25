package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class CCPathContext {
    @JsonProperty("path_hash")
    private int pathHash;

    @JsonProperty("aggregated_low_link_id")
    private String aggregatedLowLinkId;

    @JsonProperty("response_num")
    private int responseNum;

    @JsonProperty("aggregated_connected_component_ids")
    private List<String> aggregatedCCIds;   // aggregated connected component ids on the path

    public CCPathContext() {
    }

    public CCPathContext(int pathHash, String aggregatedLowLinkId, int responseNum, List<String> aggregatedCCIds) {
        this.pathHash = pathHash;
        this.aggregatedLowLinkId = aggregatedLowLinkId;
        this.responseNum = responseNum;
        this.aggregatedCCIds = aggregatedCCIds;
    }

    public int getPathHash() {
        return pathHash;
    }

    public void setPathHash(int pathHash) {
        this.pathHash = pathHash;
    }

    public String getAggregatedLowLinkId() {
        return aggregatedLowLinkId;
    }

    public void setAggregatedLowLinkId(String aggregatedLowLinkId) {
        this.aggregatedLowLinkId = aggregatedLowLinkId;
    }

    public int getResponseNum() {
        return responseNum;
    }

    public void setResponseNum(int responseNum) {
        this.responseNum = responseNum;
    }

    public List<String> getAggregatedCCIds() {
        return aggregatedCCIds;
    }

    public void setAggregatedCCIds(List<String> aggregatedCCIds) {
        this.aggregatedCCIds = aggregatedCCIds;
    }
}
