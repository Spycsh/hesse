package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

public class ForwardQueryCCWithState {
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

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    @JsonProperty("vertex_activities")
    List<VertexActivity> vertexActivities = new ArrayList<>();

    public ForwardQueryCCWithState() { }

    public ForwardQueryCCWithState(ForwardQueryCC q, List<VertexActivity> vertexActivities) {
        this.source = q.getSource();
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
        this.vertexActivities = vertexActivities;
        this.stack = q.getStack();
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

    public String getSource() {
        return source;
    }

    public List<VertexActivity> getVertexActivities() {
        return vertexActivities;
    }

    public ArrayDeque<String> getStack() {
        return stack;
    }
}