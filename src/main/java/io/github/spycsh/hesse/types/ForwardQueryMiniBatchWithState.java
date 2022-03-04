package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class ForwardQueryMiniBatchWithState {
    @JsonProperty("source")
    private String source;

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
    List<VertexActivity> vertexActivities = new ArrayList<>();

    public ForwardQueryMiniBatchWithState() {
    }

    public ForwardQueryMiniBatchWithState(String source, ForwardQueryMiniBatch q, List<VertexActivity> vertexActivities) {
        this.source = source;
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

    public String getSource() {
        return source;
    }


    public int getT() {
        return t;
    }

    public int getH() {
        return h;
    }

    public int getK() {
        return k;
    }
}
