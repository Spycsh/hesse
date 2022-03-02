package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class QueryMiniBatchWithState {
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

    @JsonProperty("vertex_activities")
    private List<VertexActivity> vertexActivities = new ArrayList<>();

    public QueryMiniBatchWithState(String queryId, String userId, String vertexId, String queryType, int t, int h, int k, List<VertexActivity> vertexActivities) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        T = t;
        H = h;
        K = k;
        this.vertexActivities = vertexActivities;
    }

    public QueryMiniBatchWithState(QueryMiniBatch q, List<VertexActivity> vertexActivities){
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.T = q.getT();
        this.H = q.getH();
        this.K = q.getK();
        this.vertexActivities = vertexActivities;
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

    public List<VertexActivity> getVertexActivities() {
        return vertexActivities;
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
