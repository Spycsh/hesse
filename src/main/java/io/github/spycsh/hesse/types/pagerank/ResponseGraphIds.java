package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

public class ResponseGraphIds {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("graph_ids")
    private Set<String> graphIds;

    public ResponseGraphIds() {
    }

    public ResponseGraphIds(String queryId, String userId, int startT, int endT, Set<String> graphIds) {
        this.queryId = queryId;
        this.userId = userId;
        this.startT = startT;
        this.endT = endT;
        this.graphIds = graphIds;
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

    public Set<String> getGraphIds() {
        return graphIds;
    }

    public void setGraphIds(Set<String> graphIds) {
        this.graphIds = graphIds;
    }
}
