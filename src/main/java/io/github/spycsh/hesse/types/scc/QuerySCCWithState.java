package io.github.spycsh.hesse.types.scc;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;

import java.util.ArrayList;
import java.util.List;

public class QuerySCCWithState {
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

    @JsonProperty("vertex_activities")
    List<VertexActivity> vertexActivities = new ArrayList<>();

    public QuerySCCWithState() { }

    public QuerySCCWithState(QuerySCC q, List<VertexActivity> vertexActivities) {
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.vertexId = q.getVertexId();
        this.queryType = q.getQueryType();
        this.t = q.getT();
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

    public int getT() {
        return t;
    }

    public List<VertexActivity> getVertexActivities() {
        return vertexActivities;
    }
}
