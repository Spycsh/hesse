package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class PageRankContext {
    @JsonProperty("query_id")
    private String queryId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("start_t")
    private int startT;

    @JsonProperty("end_t")
    private int endT;

    @JsonProperty("previous_pr_value")
    private double previousPrValue;

    @JsonProperty("current_pr_value")
    private double currentPrValue;

    @JsonProperty("current_collected_degree")
    private int currentCollectedDegree;

    @JsonProperty("in_degree")
    private int inDegree;

    @JsonProperty("neighbour_ids_with_weight")
    Map<String, String> neighbourIdsWithWeight;

    @JsonProperty("coordinator_id")
    private String coordinatorId;

    public PageRankContext() {
    }

    public PageRankContext(String queryId, String userId, int startT, int endT, double previousPrValue,
                           double currentPrValue, int currentCollectedDegree,
                           int inDegree, Map<String, String> neighbourIdsWithWeight, String coordinatorId) {
        this.queryId = queryId;
        this.userId = userId;
        this.startT = startT;
        this.endT = endT;
        this.previousPrValue = previousPrValue;
        this.currentPrValue = currentPrValue;
        this.currentCollectedDegree = currentCollectedDegree;
        this.inDegree = inDegree;
        this.neighbourIdsWithWeight = neighbourIdsWithWeight;
        this.coordinatorId = coordinatorId;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public void setCoordinatorId(String coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    public int getCurrentCollectedDegree() {
        return currentCollectedDegree;
    }

    public void setCurrentCollectedDegree(int currentCollectedDegree) {
        this.currentCollectedDegree = currentCollectedDegree;
    }

    public int getInDegree() {
        return inDegree;
    }

    public void setInDegree(int inDegree) {
        this.inDegree = inDegree;
    }

    public Map<String, String> getNeighbourIdsWithWeight() {
        return neighbourIdsWithWeight;
    }

    public void setNeighbourIdsWithWeight(Map<String, String> neighbourIdsWithWeight) {
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

    public double getCurrentPrValue() {
        return currentPrValue;
    }

    public void setCurrentPrValue(double currentPrValue) {
        this.currentPrValue = currentPrValue;
    }

    public double getPreviousPrValue() {
        return previousPrValue;
    }

    public void setPreviousPrValue(double previousPrValue) {
        this.previousPrValue = previousPrValue;
    }
}
