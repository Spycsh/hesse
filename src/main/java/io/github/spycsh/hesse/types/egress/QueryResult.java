package io.github.spycsh.hesse.types.egress;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * a general query result class
 */
public class QueryResult {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("result")
    private String result;  // a string of result for the query

    public QueryResult(String queryId, String userId, String result) {
        this.queryId = queryId;
        this.userId = userId;
        this.result = result;
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

    public String getResult() {
        return result;
    }

    public void setResult(String result) {
        this.result = result;
    }

}
