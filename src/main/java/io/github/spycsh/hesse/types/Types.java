package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.*;

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
    public static final Type<HashSet<TemporalEdge>> TEMPORAL_EDGES_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "temporal_edges"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashSet.class));

    @SuppressWarnings("unchecked")
    public static final Type<HashSet<TemporalWeightedEdge>> TEMPORAL_EDGES_WEIGHTED_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "temporal_weighted_edges"),
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

    public static final Type<TreeMap<String, ArrayList<VertexActivity>>> VERTEX_ACTIVITIES = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activities"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, TreeMap.class));

    public static final Type<HashMap<String, TreeMap<Integer, ArrayList<VertexActivity>>>> PARTITION_ACTIVITIES = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "partition_activities"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashMap.class));


    /**
     * Type denoting a query of mini batch
     * that is sent from ingress to storage
     */
    public static final Type<QueryMiniBatch> QUERY_MINI_BATCH_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryMiniBatch.class));
    /**
     * Type denoting a query of strongly connected component
     * that is sent from ingress to storage
     */
    public static final Type<QuerySCC> QUERY_SCC_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_component"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySCC.class));

    /**
     * Type denoting a query of mini batch with state
     * sent from storage to application
     */
    public static final Type<QueryMiniBatchWithState> QUERY_MINI_BATCH_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryMiniBatchWithState.class));

    /**
     * Type denoting a query of strongly connected component with state
     * sent from storage to application
     */
    public static final Type<QuerySCCWithState> QUERY_SCC_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_component_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySCCWithState.class));

    /**
     * MiniBatchFn -> neighbours' VertexStorageFn
     */
    public static final Type<ForwardQueryMiniBatch> FORWARD_QUERY_MINI_BATCH_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_mini_batch"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryMiniBatch.class));

    /**
     * neighbour's VertexStorageFn to neighbours' MiniBatchFn
     */
    public static final Type<ForwardQueryMiniBatchWithState> FORWARD_QUERY_MINI_BATCH_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_mini_batch_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryMiniBatchWithState.class));

    public static final Type<ForwardQuerySCC> FORWARD_QUERY_SCC_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_scc"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQuerySCC.class));

    public static final Type<ForwardQuerySCCWithState> FORWARD_QUERY_SCC_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_scc_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQuerySCCWithState.class));

    /**
     * query result
     */
    public static final Type<QueryMiniBatchResult> QUERY_MINI_BATCH_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryMiniBatchResult.class));

    public static final Type<QuerySCCResult> QUERY_SCC_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_scc_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySCCResult.class));

    /**
     * each query has a context on each vertex
     * so for one vertex there is a list of contexts corresponding to list of queries
     */
    public static final Type<ArrayList<QueryMiniBatchContext>> QUERY_MINI_BATCH_CONTEXT_LIST_TYPE =
        SimpleType.simpleImmutableTypeFrom(
                TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_context_list"),
                JSON_OBJ_MAPPER::writeValueAsBytes,
                bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QueryMiniBatchContext>>() {
                }));

    public static final Type<ArrayList<QuerySCCContext>> QUERY_SCC_CONTEXT_LIST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_components_context_list"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QuerySCCContext>>() {
                    }));

//    public static final Type<SCCPathContext> QUERY_SCC_PATH_CONTEXT_TYPE =
//            SimpleType.simpleImmutableTypeFrom(
//                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_components_path_context"),
//                    JSON_OBJ_MAPPER::writeValueAsBytes,
//                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, SCCPathContext.class));
}
