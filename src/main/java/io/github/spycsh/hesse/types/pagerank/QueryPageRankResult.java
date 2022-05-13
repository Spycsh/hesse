package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryPageRankResult {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("pagerank_value")
    private double pagerankValue;

    public QueryPageRankResult() {
    }

    public QueryPageRankResult(String queryId, String userId, String vertexId, double pagerankValue) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.pagerankValue = pagerankValue;
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

    public double getPagerankValue() {
        return pagerankValue;
    }

    public void setPagerankValue(double pagerankValue) {
        this.pagerankValue = pagerankValue;
    }

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }
}
