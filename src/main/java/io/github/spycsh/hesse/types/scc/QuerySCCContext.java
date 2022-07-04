package io.github.spycsh.hesse.types.scc;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;

public class QuerySCCContext {

  @JsonProperty("query_id")
  private String queryId;

  @JsonProperty("user_id")
  private String userId;

  @JsonProperty("scc_path_contexts")
  ArrayList<SCCPathContext> sccPathContexts;

  public QuerySCCContext() {}

  public QuerySCCContext(String queryId, String userId, ArrayList<SCCPathContext> sccPathContexts) {
    this.queryId = queryId;
    this.userId = userId;
    this.sccPathContexts = sccPathContexts;
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

  public ArrayList<SCCPathContext> getSccPathContexts() {
    return sccPathContexts;
  }

  public void setSccPathContexts(ArrayList<SCCPathContext> sccPathContexts) {
    this.sccPathContexts = sccPathContexts;
  }
}
