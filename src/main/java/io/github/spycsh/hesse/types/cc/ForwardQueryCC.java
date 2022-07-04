package io.github.spycsh.hesse.types.cc;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ForwardQueryCC {

  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("vertex_id")
  private String vertexId;

  @JsonProperty("start_t")
  private int startT;

  @JsonProperty("end_t")
  private int endT;

  public ForwardQueryCC() {}

  public ForwardQueryCC(String queryId, String userId, String vertexId, int startT, int endT) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
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

  public int getStartT() {
    return startT;
  }

  public int getEndT() {
    return endT;
  }
}
