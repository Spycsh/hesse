package io.github.spycsh.hesse.types.minibatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;

import java.util.ArrayDeque;
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

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("h")
    private int h;

    @JsonProperty("k")
    private int k;

    @JsonProperty("vertex_activities")
    List<VertexActivity> vertexActivities = new ArrayList<>();

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public ForwardQueryMiniBatchWithState() {
    }

    public ForwardQueryMiniBatchWithState(ForwardQueryMiniBatch q, List<VertexActivity> vertexActivities) {
        this.source = q.getSource();
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
        this.h = q.getH();
        this.k = q.getK();
        this.stack = q.getStack();
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

    public ArrayDeque<String> getStack() {
        return stack;
    }
}
