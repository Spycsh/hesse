package io.github.spycsh.hesse.types.sssp;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

public class QueryNextNeighboursInfo {
  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("vertex_id")
  private String vertexId;

  @JsonProperty("source_id")
  private String sourceId;

  @JsonProperty("neighbour_ids_with_weight")
  Map<String, String> neighbourIdsWithWeight;

  public QueryNextNeighboursInfo() {}

  public QueryNextNeighboursInfo(
      String queryId,
      String userId,
      String vertexId,
      String sourceId,
      Map<String, String> neighbourIdsWithWeight) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
    this.sourceId = sourceId;
    this.neighbourIdsWithWeight = neighbourIdsWithWeight;
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

  public String getSourceId() {
    return sourceId;
  }

  public void setSourceId(String sourceId) {
    this.sourceId = sourceId;
  }

  public Map<String, String> getNeighbourIdsWithWeight() {
    return neighbourIdsWithWeight;
  }

  public void setNeighbourIdsWithWeight(Map<String, String> neighbourIdsWithWeight) {
    this.neighbourIdsWithWeight = neighbourIdsWithWeight;
  }
}
