package io.github.spycsh.hesse.query;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.HashMap;

/** A unified class that each record in query stream will be serialized to */
public class Query {
  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("vertex_id")
  private String vertexId;

  @JsonProperty("query_type")
  private String queryType;

  @JsonProperty("start_t")
  private int startT;

  @JsonProperty("end_t")
  private int endT;

  @JsonProperty("parameter_map")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private HashMap<String, String> parameterMap;

  public Query() {}

  public Query(
      String queryId,
      String userId,
      String vertexId,
      String queryType,
      int startT,
      int endT,
      HashMap<String, String> parameterMap) {
    this.queryId = queryId;
    this.userId = userId;
    this.vertexId = vertexId;
    this.queryType = queryType;
    this.startT = startT;
    this.endT = endT;
    this.parameterMap = parameterMap;
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

  public HashMap<String, String> getParameterMap() {
    return parameterMap;
  }

  public void setParameterMap(HashMap<String, String> parameterMap) {
    this.parameterMap = parameterMap;
  }

  @Override
  public String toString() {
    return "Query{"
        + "queryId='"
        + queryId
        + '\''
        + ", userId='"
        + userId
        + '\''
        + ", vertexId='"
        + vertexId
        + '\''
        + ", queryType='"
        + queryType
        + '\''
        + ", startT="
        + startT
        + ", endT="
        + endT
        + ", parameterMap="
        + parameterMap
        + '}';
  }
}
