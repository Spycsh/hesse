package io.github.spycsh.hesse.types.gnnsampling;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.github.spycsh.hesse.types.egress.Edge;

import java.util.List;

public class GNNSamplingPathContext {

    @JsonProperty("path_hash")
    private int pathHash;

    @JsonProperty("response_num")
    private int responseNum;

    @JsonProperty("aggregated_mini_batch_edges")
    private List<Edge> aggregatedGNNSamplingEdges;



    public GNNSamplingPathContext() {
    }

    public GNNSamplingPathContext(int pathHash, int responseNum, List<Edge> aggregatedGNNSamplingEdges) {
        this.pathHash = pathHash;
        this.aggregatedGNNSamplingEdges = aggregatedGNNSamplingEdges;
        this.responseNum = responseNum;
    }

    public int getPathHash() {
        return pathHash;
    }

    public void setPathHash(int pathHash) {
        this.pathHash = pathHash;
    }

    public List<Edge> getAggregatedGNNSamplingEdges() {
        return aggregatedGNNSamplingEdges;
    }

    public void setAggregatedGNNSamplingEdges(List<Edge> aggregatedGNNSamplingEdges) {
        this.aggregatedGNNSamplingEdges = aggregatedGNNSamplingEdges;
    }

    public int getResponseNum() {
        return responseNum;
    }

    public void setResponseNum(int responseNum) {
        this.responseNum = responseNum;
    }
}
