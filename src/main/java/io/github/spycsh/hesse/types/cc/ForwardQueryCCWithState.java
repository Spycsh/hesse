package io.github.spycsh.hesse.types.cc;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.VertexActivity;
import java.util.ArrayList;
import java.util.List;

public class ForwardQueryCCWithState {

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

  public ForwardQueryCCWithState() {}

  public ForwardQueryCCWithState(ForwardQueryCC q, List<VertexActivity> vertexActivities) {
    this.queryId = q.getQueryId();
    this.userId = q.getUserId();
    this.vertexId = q.getVertexId();
    this.startT = q.getStartT();
    this.endT = q.getEndT();
    this.vertexActivities = vertexActivities;
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

  public List<VertexActivity> getVertexActivities() {
    return vertexActivities;
  }
}
