package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Set;

public class QuerySCCResult {

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

    @JsonProperty("scc_flag")
    private boolean sccFlag; // a flag represents whether on the current path the component is found

    @JsonProperty("aggregated_strongly_connected_component_ids")
    private Set<String> aggregatedSCCIds;   // all the ids belonging to one strongly connected component

    @JsonProperty("stack")
    private ArrayDeque<String> stack;

    public QuerySCCResult() {
    }

    public QuerySCCResult(String queryId, String userId, String vertexId, String queryType, String lowLinkId, Boolean sccFlag, Set<String> aggregatedSCCIds, ArrayDeque<String> stack) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.lowLinkId = lowLinkId;
        this.sccFlag = sccFlag;
        this.aggregatedSCCIds = aggregatedSCCIds;
        this.stack = stack;
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

    public String getLowLinkId() {
        return lowLinkId;
    }

    public boolean isSccFlag() {
        return sccFlag;
    }

    public Set<String> getAggregatedSCCIds() {
        return aggregatedSCCIds;
    }

    public ArrayDeque<String> getStack() {
        return stack;
    }
}
