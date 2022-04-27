package io.github.spycsh.hesse.types.sssp;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryStateRequest {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    public QueryStateRequest() {
    }

    public QueryStateRequest(String queryId, String userId, String vertexId, int startT, int endT) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.startT = startT;
        this.endT = endT;
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

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
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
}
