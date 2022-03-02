package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryMiniBatchSourceId {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("source_id")
    private String sourceId;

    public QueryMiniBatchSourceId(String queryId, String userId, String sourceId) {
        this.queryId = queryId;
        this.userId = userId;
        this.sourceId = sourceId;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getUserId() {
        return userId;
    }

    public String getSourceId() {
        return sourceId;
    }
}
