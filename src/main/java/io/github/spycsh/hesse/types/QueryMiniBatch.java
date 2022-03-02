package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryMiniBatch {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("query_type")
    private String queryType;

    @JsonProperty("T")
    private int T;

    @JsonProperty("H")
    private int H;

    @JsonProperty("K")
    private int K;

    public QueryMiniBatch(String queryId, String userId, String vertexId, String queryType, int t, int h, int k) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        T = t;
        H = h;
        K = k;
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

    public int getT() {
        return T;
    }

    public int getH() {
        return H;
    }

    public int getK() {
        return K;
    }
}
