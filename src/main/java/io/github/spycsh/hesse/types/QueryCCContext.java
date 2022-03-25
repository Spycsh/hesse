package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;

public class QueryCCContext {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("cc_path_contexts")
    ArrayList<CCPathContext> ccPathContexts;

    public QueryCCContext() {
    }

    public QueryCCContext(String queryId, String userId, ArrayList<CCPathContext> ccPathContexts) {
        this.queryId = queryId;
        this.userId = userId;
        this.ccPathContexts = ccPathContexts;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getUserId() {
        return userId;
    }

    public ArrayList<CCPathContext> getCcPathContexts() {
        return ccPathContexts;
    }
}
