package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

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

    public QuerySCCResult() {
    }

    public QuerySCCResult(String queryId, String userId, String vertexId, String queryType, String lowLinkId, Boolean sccFlag) {
        this.queryId = queryId;
        this.userId = userId;
        this.vertexId = vertexId;
        this.queryType = queryType;
        this.lowLinkId = lowLinkId;
        this.sccFlag = sccFlag;
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
}
