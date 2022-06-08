package io.github.spycsh.hesse.types.cc;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.HashSet;

public class QueryCCContext {

    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("vertex_set")
    private HashSet<String> vertexSet;

    @JsonProperty("candidate_neighbours")
    private HashSet<String> candidateNeighbours;

    @JsonProperty("candidate_response_number")
    private int candidateResponseNumber;

    public QueryCCContext() {
    }

    public QueryCCContext(String queryId, String userId, int startT, int endT, HashSet<String> vertexSet, HashSet<String> candidateNeighbours, int candidateResponseNumber) {
        this.queryId = queryId;
        this.userId = userId;
        this.startT = startT;
        this.endT = endT;
        this.vertexSet = vertexSet;
        this.candidateNeighbours = candidateNeighbours;
        this.candidateResponseNumber = candidateResponseNumber;
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

    public String getUserId() {
        return userId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
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

    public int getCandidateResponseNumber() {
        return candidateResponseNumber;
    }

    public void setCandidateResponseNumber(int candidateResponseNumber) {
        this.candidateResponseNumber = candidateResponseNumber;
    }

    public HashSet<String> getCandidateNeighbours() {
        return candidateNeighbours;
    }

    public void setCandidateNeighbours(HashSet<String> candidateNeighbours) {
        this.candidateNeighbours = candidateNeighbours;
    }
}
