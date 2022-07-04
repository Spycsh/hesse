package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HistoryQuery {
  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("vertex_id")
  private String vertexId;

  @JsonProperty("query_type")
  private String queryType;

  @JsonProperty("query_receive_time")
  private long queryReceiveTime;

  @JsonProperty("result")
  private String result; // a string of result for the query

  public HistoryQuery() {}

  public HistoryQuery(
      String queryId, String userId, String vertexId, String queryType, long queryReceiveTime) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
    this.queryType = queryType;
    this.queryReceiveTime = queryReceiveTime;
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

  public long getQueryReceiveTime() {
    return queryReceiveTime;
  }

  public void setQueryReceiveTime(long queryReceiveTime) {
    this.queryReceiveTime = queryReceiveTime;
  }

  public String getResult() {
    return result;
  }

  public void setResult(String result) {
    this.result = result;
  }
}
