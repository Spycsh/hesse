package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.util.ArrayList;

public class QuerySCCContext {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("response_num_to_collect")
    private int responseNumToCollect;

    @JsonProperty("aggregated_low_link_id")
    private String aggregatedLowLinkId;

    @JsonProperty("scc_flag")
    private boolean sccFlag;    // whether there is a connected component found currently

    @JsonProperty("source_ids")
    private ArrayList<String> sourceIds;


    public QuerySCCContext() {
    }

    public QuerySCCContext(String queryId, String userId, int responseNumToCollect, String aggregatedLowLinkId, ArrayList<String> sourceIds, Boolean sccFlag){
        this.queryId = queryId;
        this.userId = userId;
        this.responseNumToCollect = responseNumToCollect;
        this.aggregatedLowLinkId = aggregatedLowLinkId;
        this.sourceIds = sourceIds;
        this.sccFlag = sccFlag;

    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getResponseNumToCollect() {
        return responseNumToCollect;
    }

    public void setResponseNumToCollect(int responseNumToCollect) {
        this.responseNumToCollect = responseNumToCollect;
    }

    public ArrayList<String> getSourceIds() {
        return sourceIds;
    }

    public void setSourceIds(ArrayList<String> sourceIds) {
        this.sourceIds = sourceIds;
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
}
