package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

/**
 * this class records a vertex's parents for each query
 * and the number of responses to collect
 */
public class QueryMiniBatchContext {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("response_num_to_collect")
    private int responseNumToCollect;

    @JsonProperty("source_ids")
    private ArrayList<String> sourceIds;

    @JsonProperty("current_aggregated_results")
    private ArrayList<Edge> currentAggregatedResults;

    public QueryMiniBatchContext() {
    }

    public QueryMiniBatchContext(String queryId, String userId, int responseNumToCollect, ArrayList<String> sourceIds, ArrayList<Edge> currentAggregatedResults) {
        this.queryId = queryId;
        this.userId = userId;
        this.responseNumToCollect = responseNumToCollect;
        this.sourceIds = sourceIds;
        this.currentAggregatedResults = currentAggregatedResults;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getUserId() {
        return userId;
    }

    public int getResponseNumToCollect() {
        return responseNumToCollect;
    }

    public ArrayList<Edge> getCurrentAggregatedResults() {
        return currentAggregatedResults;
    }

    public void setCurrentAggregatedResults(ArrayList<Edge> currentAggregatedResults) {
        this.currentAggregatedResults = currentAggregatedResults;
    }

    public ArrayList<String> getSourceIds() {
        return sourceIds;
    }

//    public void setSourceIds(ArrayList<String> sourceIds) {
//        this.sourceIds = sourceIds;
//    }

    public void setResponseNumToCollect(int responseNumToCollect) {
        this.responseNumToCollect = responseNumToCollect;
    }
}
