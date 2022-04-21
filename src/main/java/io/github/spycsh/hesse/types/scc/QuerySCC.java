package io.github.spycsh.hesse.types.scc;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QuerySCC {
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

    public QuerySCC() { }

    public QuerySCC(String queryId, String userId, String vertexId, String queryType, int startT, int endT) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.startT = startT;
        this.endT = endT;
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
}
