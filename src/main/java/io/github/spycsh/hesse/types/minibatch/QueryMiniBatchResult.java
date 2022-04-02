package io.github.spycsh.hesse.types.minibatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.egress.Edge;

import java.util.ArrayDeque;
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

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public QueryMiniBatchResult() {
    }

    public QueryMiniBatchResult(String queryId, String userId, String vertexId, String queryType,ArrayList<Edge> aggregatedResults, ArrayDeque<String> stack) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.aggregatedResults = aggregatedResults;
        this.stack = stack;
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

    public ArrayDeque<String> getStack() {
        return stack;
    }
}
