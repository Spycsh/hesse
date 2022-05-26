package io.github.spycsh.hesse.types.gnnsampling;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;

import java.util.ArrayList;
import java.util.List;

public class QueryGNNSamplingState {
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

    @JsonProperty("vertex_activities")
    private List<VertexActivity> vertexActivities = new ArrayList<>();

    public QueryGNNSamplingState() {
    }

    public QueryGNNSamplingState(String queryId, String userId, String vertexId, String queryType, int startT, int endT, int h, int k, List<VertexActivity> vertexActivities) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.startT = startT;
        this.endT = endT;
        this.h = h;
        this.k = k;
        this.vertexActivities = vertexActivities;
    }

    public QueryGNNSamplingState(QueryGNNSampling q, List<VertexActivity> vertexActivities){
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
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

    public int getStartT() {
        return startT;
    }

    public int getEndT() {
        return endT;
    }

    public int getH() {
        return this.h;
    }

    public int getK() {
        return this.k;
    }
}
