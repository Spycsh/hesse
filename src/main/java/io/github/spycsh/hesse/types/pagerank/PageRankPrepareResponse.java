package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageRankPrepareResponse {

  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  public PageRankPrepareResponse() {}

  public PageRankPrepareResponse(String queryId, String userId) {
    this.queryId = queryId;
    this.userId = userId;
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
}
