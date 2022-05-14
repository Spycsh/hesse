package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryGraphIds {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("source_id")
    private String sourceId;

    public QueryGraphIds() {
    }

    public QueryGraphIds(String queryId, String userId, int startT, int endT, String sourceId) {
        this.queryId = queryId;
        this.userId = userId;
        this.startT = startT;
        this.endT = endT;
        this.sourceId = sourceId;
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

    public int getStartT() {
        return startT;
    }

    public void setStartT(int startT) {
        this.startT = startT;
    }

    public int getEndT() {
        return endT;
    }

    public void setEndT(int endT) {
        this.endT = endT;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }
}
