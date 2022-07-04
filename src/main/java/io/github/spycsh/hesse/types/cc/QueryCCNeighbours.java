package io.github.spycsh.hesse.types.cc;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;

public class QueryCCNeighbours {

  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("vertex_id")
  private String vertexId;

  @JsonProperty("neighbours")
  private HashSet<String> neighbours;

  public QueryCCNeighbours() {}

  public QueryCCNeighbours(
      String queryId, String userId, String vertexId, HashSet<String> neighbours) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
    this.neighbours = neighbours;
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

  public HashSet<String> getNeighbours() {
    return neighbours;
  }

  public void setNeighbours(HashSet<String> neighbours) {
    this.neighbours = neighbours;
  }
}
