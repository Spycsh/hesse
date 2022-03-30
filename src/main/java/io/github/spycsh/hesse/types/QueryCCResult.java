package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;

public class QueryCCResult {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("vertex_id")
    private String vertexId;

    @JsonProperty("query_type")
    private String queryType;

    @JsonProperty("low_link_id")
    private String lowLinkId;  // minimum component id aggregated from children nodes

    @JsonProperty("aggregated_connected_component_ids")
    private Set<String> aggregatedCCIds;

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public QueryCCResult() {
    }

    public QueryCCResult(String queryId, String userId, String vertexId, String queryType, String lowLinkId, Set<String> aggregatedCCIds, ArrayDeque<String> stack) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.lowLinkId = lowLinkId;
        this.aggregatedCCIds = aggregatedCCIds;
        this.stack = stack;
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

    public String getVertexId() {
        return vertexId;
    }

    public void setVertexId(String vertexId) {
        this.vertexId = vertexId;
    }

    public String getQueryType() {
        return queryType;
    }

    public void setQueryType(String queryType) {
        this.queryType = queryType;
    }

    public String getLowLinkId() {
        return lowLinkId;
    }

    public void setLowLinkId(String lowLinkId) {
        this.lowLinkId = lowLinkId;
    }

    public Set<String> getAggregatedCCIds() {
        return aggregatedCCIds;
    }

    public void setAggregatedCCIds(Set<String> aggregatedCCIds) {
        this.aggregatedCCIds = aggregatedCCIds;
    }

    public ArrayDeque<String> getStack() {
        return stack;
    }

    public void setStack(ArrayDeque<String> stack) {
        this.stack = stack;
    }
}
