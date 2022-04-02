package io.github.spycsh.hesse.types.minibatch;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.egress.Edge;

import java.util.List;

public class MiniBatchPathContext {

    @JsonProperty("path_hash")
    private int pathHash;

    @JsonProperty("response_num")
    private int responseNum;

    @JsonProperty("aggregated_mini_batch_edges")
    private List<Edge> aggregatedMiniBatchEdges;



    public MiniBatchPathContext() {
    }

    public MiniBatchPathContext(int pathHash,  int responseNum, List<Edge> aggregatedMiniBatchEdges) {
        this.pathHash = pathHash;
        this.aggregatedMiniBatchEdges = aggregatedMiniBatchEdges;
        this.responseNum = responseNum;
    }

    public int getPathHash() {
        return pathHash;
    }

    public void setPathHash(int pathHash) {
        this.pathHash = pathHash;
    }

    public List<Edge> getAggregatedMiniBatchEdges() {
        return aggregatedMiniBatchEdges;
    }

    public void setAggregatedMiniBatchEdges(List<Edge> aggregatedMiniBatchEdges) {
        this.aggregatedMiniBatchEdges = aggregatedMiniBatchEdges;
    }

    public int getResponseNum() {
        return responseNum;
    }

    public void setResponseNum(int responseNum) {
        this.responseNum = responseNum;
    }
}
