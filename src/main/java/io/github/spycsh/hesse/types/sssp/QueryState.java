package io.github.spycsh.hesse.types.sssp;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;
import java.util.ArrayList;
import java.util.List;

public class QueryState {
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

  @JsonProperty("vertex_activities")
  List<VertexActivity> vertexActivities = new ArrayList<>();

  public QueryState() {}

  public QueryState(
      String queryId,
      String userId,
      String vertexId,
      int startT,
      int endT,
      List<VertexActivity> vertexActivities) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
    this.startT = startT;
    this.endT = endT;
    this.vertexActivities = vertexActivities;
  }

  public String getVertexId() {
    return vertexId;
  }

  public void setVertexId(String vertexId) {
    this.vertexId = vertexId;
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
