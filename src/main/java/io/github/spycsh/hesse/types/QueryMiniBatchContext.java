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

    @JsonProperty("mini_batch_path_contexts")
    ArrayList<MiniBatchPathContext> miniBatchPathContexts;


    public QueryMiniBatchContext() {
    }

    public QueryMiniBatchContext(String queryId, String userId, ArrayList<MiniBatchPathContext> miniBatchPathContexts) {
        this.queryId = queryId;
        this.userId = userId;
        this.miniBatchPathContexts = miniBatchPathContexts;
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

    public ArrayList<MiniBatchPathContext> getMiniBatchPathContexts() {
        return miniBatchPathContexts;
    }

    public void setMiniBatchPathContexts(ArrayList<MiniBatchPathContext> miniBatchPathContexts) {
        this.miniBatchPathContexts = miniBatchPathContexts;
    }
}
