package io.github.spycsh.hesse.types.minibatch;

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

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("h")
    private int h;

    @JsonProperty("k")
    private int k;

    public QueryMiniBatch() {}

    public QueryMiniBatch(String queryId, String userId, String vertexId, String queryType, int startT, int endT, int h, int k) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.startT = startT;
        this.endT = endT;
        this.h = h;
        this.k = k;
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

    public int getStartT() {
        return startT;
    }

    public int getEndT() {
        return endT;
    }

    public int getH() {
        return h;
    }

    public int getK() {
        return k;
    }
}
