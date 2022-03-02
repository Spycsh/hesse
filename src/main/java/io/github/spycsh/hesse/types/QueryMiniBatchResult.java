package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryMiniBatchResult {
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

    public QueryMiniBatchResult(String source, String target, String queryId, String userId, String vertexId, String queryType) {
        this.source = source;
        this.target = target;
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
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

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }
}
