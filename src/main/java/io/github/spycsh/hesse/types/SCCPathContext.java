package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;


public class SCCPathContext {
    @JsonProperty("path_hash")
    private int pathHash;

    @JsonProperty("aggregated_low_link_id")
    private String aggregatedLowLinkId;

    @JsonProperty("scc_flag")
    private boolean sccFlag;    // whether there is a connected component found currently

    @JsonProperty("response_num")
    private int responseNum;

    public SCCPathContext() {
    }

    public SCCPathContext(int pathHash, String aggregatedLowLinkId, boolean sccFlag, int responseNum) {
        this.pathHash = pathHash;
        this.aggregatedLowLinkId = aggregatedLowLinkId;
        this.sccFlag = sccFlag;
        this.responseNum = responseNum;
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
}
