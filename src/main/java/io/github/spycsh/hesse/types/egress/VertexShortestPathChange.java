package io.github.spycsh.hesse.types.egress;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;

public class VertexShortestPathChange {
    @JsonProperty("weight")
    private double weight;

    @JsonProperty("shortest_distances")
    private HashMap<String, String> shortestPathDistances;

    public VertexShortestPathChange() {
        this(new HashMap<>(), 1.0);
    }

    public HashMap<String, String> getShortestPathDistances() {
        return shortestPathDistances;
    }

    public double getWeight() {
        return weight;
    }

    public VertexShortestPathChange(HashMap<String, String> shortestPathDistances, double weight) {
        this.shortestPathDistances = shortestPathDistances;
        this.weight = weight;
    }

    public static VertexShortestPathChange create(HashMap<String, String> shortestPathDistances, double weight) {
        return new VertexShortestPathChange(shortestPathDistances, weight);
    }
}
