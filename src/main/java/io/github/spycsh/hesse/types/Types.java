package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.spycsh.hesse.PartitionManagerFn;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class Types {
    private Types() {}

    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();
    private static final String TYPES_NAMESPACE = "hesse.types";

    /**
     * denote the partition id
     */
    public static final Type<PartitionConfig> PARTITION_CONFIG_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "partition_config"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PartitionConfig.class));

    /**
     * Type denoting a new edge coming from the input source.
     */
    public static final Type<TemporalEdge> TEMPORAL_EDGE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "temporal_edge"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, TemporalEdge.class));

    public static final Type<TemporalWeightedEdge> TEMPORAL_EDGE_WEIGHTED_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "temporal_edge_weighted"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, TemporalWeightedEdge.class));


    /**
     * denote a list of edges hold by one partition
     */
    @SuppressWarnings("unchecked")
    public static final Type<Set<TemporalEdge>> TEMPORAL_EDGES_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "temporal_edges"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashSet.class));

    @SuppressWarnings("unchecked")
    public static final Type<HashSet<Integer>> NEIGHBOURS_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "neighbours"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashSet.class));

    @SuppressWarnings("unchecked")
    public static final Type<HashMap<Integer, Double>> NEIGHBOURS_WEIGHTED_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "neighbours_weighted"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashMap.class));

    public static final Type<VertexComponentChange> VERTEX_COMPONENT_CHANGE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "vertexComponentChange"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, VertexComponentChange.class));

    @SuppressWarnings("unchecked")
    public static final Type<HashSet<Integer>> BUFFERED_NEIGHBOURS_VALUE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "buffered_neighbours"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashSet.class));


    @SuppressWarnings("unchecked")
    public static final Type<HashMap<Integer, Double>> BUFFERED_NEIGHBOURS_WEIGHTED_VALUE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "buffered_neighbours_weighted"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashMap.class));

    @SuppressWarnings("unchecked")
    public static final Type<HashMap<String, String>> SHORTEST_PATH_DISTANCES_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "shortest_path_distances"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashMap.class));

    @SuppressWarnings("unchecked")
    public static final Type<VertexShortestPathChange> VERTEX_SHORTEST_PATH_CHANGE_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_shortest_path_changes"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<VertexShortestPathChange>() {
            }));

}
