package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

public class QueryMiniBatchResult {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("query_type")
    private String queryType;

    @JsonProperty("aggregated_results")
    private ArrayList<Edge> aggregatedResults;  // aggregated results sent from children nodes

    public QueryMiniBatchResult() {
    }

    public QueryMiniBatchResult(String queryId, String userId, String vertexId, String queryType,ArrayList<Edge> aggregatedResults) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.aggregatedResults = aggregatedResults;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getUserId() {
        return userId;
    }

    public String getVertexId() {
        return vertexId;
    }

    public String getQueryType() {
        return queryType;
    }

    public ArrayList<Edge> getAggregatedResults() {
        return aggregatedResults;
    }

//    public void setAggregatedResults(ArrayList<Edge> aggregatedResults) {
//        this.aggregatedResults = aggregatedResults;
//    }
}
