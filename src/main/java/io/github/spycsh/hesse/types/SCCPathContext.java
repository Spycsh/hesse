package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;


public class SCCPathContext {
    @JsonProperty("path_hash")
    private int pathHash;

    @JsonProperty("aggregated_low_link_id")
    private String aggregatedLowLinkId;

    @JsonProperty("scc_flag")
    private boolean sccFlag;    // whether there is a connected component found currently

    @JsonProperty("response_num")
    private int responseNum;

    @JsonProperty("aggregated_strongly_connected_component_ids")
    private Set<String> aggregatedSCCIds;   // aggregated strongly connected component ids on the path

    public SCCPathContext() {
    }

    public SCCPathContext(int pathHash, String aggregatedLowLinkId, boolean sccFlag, int responseNum, Set<String> aggregatedSCCIds) {
        this.pathHash = pathHash;
        this.aggregatedLowLinkId = aggregatedLowLinkId;
        this.sccFlag = sccFlag;
        this.responseNum = responseNum;
        this.aggregatedSCCIds = aggregatedSCCIds;
    }

    public String getAggregatedLowLinkId() {
        return aggregatedLowLinkId;
    }

    public void setAggregatedLowLinkId(String aggregatedLowLinkId) {
        this.aggregatedLowLinkId = aggregatedLowLinkId;
    }

    public boolean isSccFlag() {
        return sccFlag;
    }

    public void setSccFlag(boolean sccFlag) {
        this.sccFlag = sccFlag;
    }

    public int getPathHash() {
        return pathHash;
    }

    public void setPathHash(int pathHash) {
        this.pathHash = pathHash;
    }

    public int getResponseNum() {
        return responseNum;
    }

    public void setResponseNum(int responseNum) {
        this.responseNum = responseNum;
    }

    public Set<String> getAggregatedSCCIds() {
        return aggregatedSCCIds;
    }

    public void setAggregatedSCCIds(Set<String> aggregatedSCCIds) {
        this.aggregatedSCCIds = aggregatedSCCIds;
    }
}
