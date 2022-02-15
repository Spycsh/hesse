package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.concurrent.ConcurrentHashMap;

public class VertexShortestPathChange {
    @JsonProperty("shortest_path_distances")
    private ConcurrentHashMap<Integer, Double> shortestPathDistances;

    @JsonProperty("weight")
    private double weight;

    public VertexShortestPathChange() {
        this(new ConcurrentHashMap<>(), 1.0);
    }

    public ConcurrentHashMap<Integer, Double> getShortestPathDistances() {
        return shortestPathDistances;
    }

    public double getWeight() {
        return weight;
    }

    public VertexShortestPathChange(ConcurrentHashMap<Integer, Double> shortestPathDistances, double weight) {
        this.shortestPathDistances = shortestPathDistances;
        this.weight = weight;
    }

    public static VertexShortestPathChange create(ConcurrentHashMap<Integer, Double> shortestPathDistances, double weight) {
        return new VertexShortestPathChange(shortestPathDistances, weight);
    }
}
