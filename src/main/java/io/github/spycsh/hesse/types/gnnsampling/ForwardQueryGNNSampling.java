package io.github.spycsh.hesse.types.gnnsampling;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;

public class ForwardQueryGNNSampling {
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

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("h")
    private int h;

    @JsonProperty("k")
    private int k;

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public ForwardQueryGNNSampling() {
    }

    public ForwardQueryGNNSampling(String source, String target, QueryGNNSamplingState q, int newHopNum, ArrayDeque<String> stack) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
        this.h = q.getH();
        this.k = newHopNum;
        this.stack = stack;
    }

    public ForwardQueryGNNSampling(String source, String target, ForwardQueryGNNSamplingWithState q, int newHopNum, ArrayDeque<String> stack) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
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
