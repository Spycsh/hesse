package io.github.spycsh.hesse.types.gnnsampling;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

/**
 * this class records a vertex's parents for each query
 * and the number of responses to collect
 */
public class QueryGNNSamplingContext {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("mini_batch_path_contexts")
    ArrayList<GNNSamplingPathContext> GNNSamplingPathContexts;


    public QueryGNNSamplingContext() {
    }

    public QueryGNNSamplingContext(String queryId, String userId, ArrayList<GNNSamplingPathContext> GNNSamplingPathContexts) {
        this.queryId = queryId;
        this.userId = userId;
        this.GNNSamplingPathContexts = GNNSamplingPathContexts;
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

    public ArrayList<GNNSamplingPathContext> getGNNSamplingPathContexts() {
        return GNNSamplingPathContexts;
    }

    public void setGNNSamplingPathContexts(ArrayList<GNNSamplingPathContext> GNNSamplingPathContexts) {
        this.GNNSamplingPathContexts = GNNSamplingPathContexts;
    }
}
