package io.github.spycsh.hesse.types.minibatch;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;

public class ForwardQueryMiniBatch {
    @JsonProperty("source")
    private String source;

    @JsonProperty("target")
    private String target;

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("query_type")
    private String queryType;

    @JsonProperty("t")
    int t;

    @JsonProperty("h")
    int h;

    @JsonProperty("k")
    int k;

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public ForwardQueryMiniBatch() {
    }

    public ForwardQueryMiniBatch(String source, String target, QueryMiniBatchWithState q, int newHopNum, ArrayDeque<String> stack) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.t = q.getT();
        this.h = q.getH();
        this.k = newHopNum;
        this.stack = stack;
    }

    public ForwardQueryMiniBatch(String source, String target, ForwardQueryMiniBatchWithState q, int newHopNum, ArrayDeque<String> stack) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.t = q.getT();
        this.h = q.getH();
        this.k = newHopNum;
        this.stack = stack;
    }

    public String getQueryId() {
        return queryId;
    }

    public String getUserId() {
        return userId;
    }

    public String getQueryType() {
        return queryType;
    }

    public String getVertexId() {
        return vertexId;
    }

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
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

    public ArrayDeque<String> getStack() {
        return stack;
    }
}
