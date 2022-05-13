package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;

import java.util.ArrayList;
import java.util.List;

public class PageRankTaskWithState {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("vertex_activities")
    List<VertexActivity> vertexActivities = new ArrayList<>();

    public PageRankTaskWithState() {
    }

    public PageRankTaskWithState(PageRankTask q, List<VertexActivity> vertexActivities){
        this.queryId = q.getQueryId();
        this.userId = q.getUserId();
        this.startT = q.getStartT();
        this.endT = q.getEndT();
        this.vertexActivities = vertexActivities;
    }

    public PageRankTaskWithState(String queryId, String userId, int startT, int endT, int inDegree, List<VertexActivity> vertexActivities) {
        this.queryId = queryId;
        this.userId = userId;
        this.startT = startT;
        this.endT = endT;
        this.vertexActivities = vertexActivities;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public int getStartT() {
        return startT;
    }

    public void setStartT(int startT) {
        this.startT = startT;
    }

    public int getEndT() {
        return endT;
    }

    public void setEndT(int endT) {
        this.endT = endT;
    }

    public List<VertexActivity> getVertexActivities() {
        return vertexActivities;
    }

    public void setVertexActivities(List<VertexActivity> vertexActivities) {
        this.vertexActivities = vertexActivities;
    }
}
