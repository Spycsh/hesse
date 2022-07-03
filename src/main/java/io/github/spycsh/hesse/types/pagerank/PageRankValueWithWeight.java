package io.github.spycsh.hesse.types.pagerank;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PageRankValueWithWeight {
    @JsonProperty("query_id")
    String queryId;

    @JsonProperty("user_id")
    String userId;

    @JsonProperty("pagerank_value")
    double pagerankValue;

    @JsonProperty("weight")
    double weight;

    @JsonProperty("weight_sum")
    double weightSum;

    public PageRankValueWithWeight() {
    }

    public PageRankValueWithWeight(String queryId, String userId, double pagerankValue, double weight, double weightSum) {
        this.queryId = queryId;
        this.userId = userId;
        this.pagerankValue = pagerankValue;
        this.weight = weight;
        this.weightSum = weightSum;
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

    public double getPagerankValue() {
        return pagerankValue;
    }

    public void setPagerankValue(double pagerankValue) {
        this.pagerankValue = pagerankValue;
    }

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public double getWeightSum() {
        return weightSum;
    }

    public void setWeightSum(double weightSum) {
        this.weightSum = weightSum;
    }
}
