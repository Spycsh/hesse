package io.github.spycsh.hesse.types.sssp;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class QuerySSSPContext {
  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("start_t")
  private int startT;

  @JsonProperty("end_t")
  private int endT;

  @JsonProperty("vertex_set")
  HashSet<String> vertexSet;

  @JsonProperty("neighbour_ids_with_weight")
  Map<String, String> neighbourIdsWithWeight;

  @JsonProperty("paths")
  Map<String, List<String>> paths;

  public QuerySSSPContext() {}

  public QuerySSSPContext(
      String queryId,
      String userId,
      int startT,
      int endT,
      HashSet<String> vertexSet,
      Map<String, String> neighbourIdsWithWeight,
      Map<String, List<String>> paths) {
    this.queryId = queryId;
    this.userId = userId;
    this.startT = startT;
    this.endT = endT;
    this.vertexSet = vertexSet;
    this.neighbourIdsWithWeight = neighbourIdsWithWeight;
    this.paths = paths;
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

  public HashSet<String> getVertexSet() {
    return vertexSet;
  }

  public void setVertexSet(HashSet<String> vertexSet) {
    this.vertexSet = vertexSet;
  }

  public Map<String, String> getNeighbourIdsWithWeight() {
    return neighbourIdsWithWeight;
  }

  public void setNeighbourIdsWithWeight(Map<String, String> neighbourIdsWithWeight) {
    this.neighbourIdsWithWeight = neighbourIdsWithWeight;
  }

  public Map<String, List<String>> getPaths() {
    return paths;
  }

  public void setPaths(Map<String, List<String>> paths) {
    this.paths = paths;
  }
}
