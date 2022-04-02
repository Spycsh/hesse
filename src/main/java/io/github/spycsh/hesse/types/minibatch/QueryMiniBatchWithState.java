package io.github.spycsh.hesse.types.minibatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;

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

    @JsonProperty("t")
    private int t;

    @JsonProperty("h")
    private int h;

    @JsonProperty("k")
    private int k;

    @JsonProperty("vertex_activities")
    private List<VertexActivity> vertexActivities = new ArrayList<>();

    public QueryMiniBatchWithState() {
    }

    public QueryMiniBatchWithState(String queryId, String userId, String vertexId, String queryType, int t, int h, int k, List<VertexActivity> vertexActivities) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.t = t;
        this.h = h;
        this.k = k;
        this.vertexActivities = vertexActivities;
    }

    public QueryMiniBatchWithState(QueryMiniBatch q, List<VertexActivity> vertexActivities){
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.t = q.getT();
        this.h = q.getH();
        this.k = q.getK();
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
        return this.t;
    }

    public int getH() {
        return this.h;
    }

    public int getK() {
        return this.k;
    }
}
