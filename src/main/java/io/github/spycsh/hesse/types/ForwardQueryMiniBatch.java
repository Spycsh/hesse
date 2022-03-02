package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    @JsonProperty("T")
    int T;

    @JsonProperty("H")
    int H;

    @JsonProperty("K")
    int K;

    public ForwardQueryMiniBatch(String source, String target, QueryMiniBatchWithState q, int newHopNum) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.T = q.getT();
        this.H = q.getH();
        this.K = newHopNum;
    }

    public ForwardQueryMiniBatch(String source, String target, ForwardQueryMiniBatchWithState q, int newHopNum) {
        this.source = source;
        this.target = target;
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.T = q.getT();
        this.H = q.getH();
        this.K = newHopNum;
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
        return T;
    }

    public int getH() {
        return H;
    }

    public int getK() {
        return K;
    }
}
