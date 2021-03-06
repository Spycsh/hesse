package io.github.spycsh.hesse.types.scc;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayDeque;

public class ForwardQuerySCC {
  @JsonProperty("source")
  private String source;

  @JsonProperty("target")
  private String target;

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

  @JsonProperty("stack")
  private ArrayDeque<String> stack;

  public ForwardQuerySCC() {}

  public ForwardQuerySCC(
      String source, String target, ArrayDeque<String> stack, QuerySCCWithState q) {
    this.source = source;
    this.target = target;
    this.queryId = q.getQueryId();
    this.userId = q.getUserId();
    this.vertexId = q.getVertexId();
    this.queryType = q.getQueryType();
    this.startT = q.getStartT();
    this.endT = q.getEndT();
    this.stack = stack;
  }

  public ForwardQuerySCC(
      String source, String target, ArrayDeque<String> stack, ForwardQuerySCCWithState q) {
    this.source = source;
    this.target = target;
    this.queryId = q.getQueryId();
    this.userId = q.getUserId();
    this.vertexId = q.getVertexId();
    this.queryType = q.getQueryType();
    this.startT = q.getStartT();
    this.endT = q.getEndT();
    this.stack = stack;
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

  public String getQueryType() {
    return queryType;
  }

  public int getStartT() {
    return startT;
  }

  public int getEndT() {
    return endT;
  }

  public String getSource() {
    return source;
  }

  public String getTarget() {
    return target;
  }

  public ArrayDeque<String> getStack() {
    return stack;
  }
}
