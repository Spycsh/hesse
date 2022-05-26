package io.github.spycsh.hesse.types;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.github.spycsh.hesse.query.Query;
import io.github.spycsh.hesse.types.cc.*;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.egress.VertexComponentChange;
import io.github.spycsh.hesse.types.egress.VertexShortestPathChange;
import io.github.spycsh.hesse.types.ingress.TemporalEdge;
import io.github.spycsh.hesse.types.gnnsampling.*;
import io.github.spycsh.hesse.types.pagerank.*;
import io.github.spycsh.hesse.types.scc.*;
import io.github.spycsh.hesse.types.sssp.*;
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

    public static final Type<VertexActivity> VERTEX_ACTIVITY_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activity"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, VertexActivity.class));

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

    public static final Type<HashMap<String, TreeMap<Integer, ArrayList<VertexActivity>>>> PARTITION_ACTIVITIES = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "partition_activities"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, HashMap.class));


    /**
     * Type denoting a query of mini batch
     * that is sent from ingress to storage
     */
    public static final Type<QueryGNNSampling> QUERY_GNN_SAMPLING_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryGNNSampling.class));
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
     * Type denoting a query of single source shortest path
     * that is sent from ingress to storage
     */
    public static final Type<QuerySSSP> QUERY_SSSP_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_single_source_shortest_path"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySSSP.class));

    /**
     * Type denoting a query of connected component
     * that is sent from ingress to storage
     */
    public static final Type<QueryCC> QUERY_CC_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_connected_component"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryCC.class));

    public static final Type<QueryPageRank> QUERY_PAGERANK_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_pagerank"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryPageRank.class));

    /**
     * Type denoting a query of mini batch with state
     * sent from storage to application
     */
    public static final Type<QueryGNNSamplingState> QUERY_GNN_SAMPLING_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryGNNSamplingState.class));

    /**
     * Type denoting a query of strongly connected component with state
     * sent from storage to application
     */
    public static final Type<QuerySCCWithState> QUERY_SCC_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_component_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySCCWithState.class));

    public static final Type<QueryCCWithState> QUERY_CC_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_connected_component_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryCCWithState.class));

    public static final Type<QuerySSSPWithState> QUERY_SSSP_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_single_source_shortest_path_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySSSPWithState.class));

    /**
     * GNNSamplingFn -> neighbours' VertexStorageFn
     */
    public static final Type<ForwardQueryGNNSampling> FORWARD_QUERY_GNN_SAMPLING_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_mini_batch"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryGNNSampling.class));

    /**
     * neighbour's VertexStorageFn to neighbours' GNNSamplingFn
     */
    public static final Type<ForwardQueryGNNSamplingWithState> FORWARD_QUERY_GNN_SAMPLING_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_mini_batch_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryGNNSamplingWithState.class));

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

    public static final Type<ForwardQueryCC> FORWARD_QUERY_CC_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_cc"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryCC.class));

    public static final Type<ForwardQueryCCWithState> FORWARD_QUERY_CC_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "forward_query_cc_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ForwardQueryCCWithState.class));

    /**
     * query result
     */
    public static final Type<QueryGNNSamplingResult> QUERY_GNN_SAMPLING_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryGNNSamplingResult.class));

    public static final Type<QuerySCCResult> QUERY_SCC_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_scc_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QuerySCCResult.class));

    public static final Type<QueryCCResult> QUERY_CC_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_cc_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryCCResult.class));

    /**
     * each query has a context on each vertex
     * so for one vertex there is a list of contexts corresponding to list of queries
     */
    public static final Type<ArrayList<QueryGNNSamplingContext>> QUERY_GNN_SAMPLING_CONTEXT_LIST_TYPE =
        SimpleType.simpleImmutableTypeFrom(
                TypeName.typeNameOf(TYPES_NAMESPACE, "query_mini_batch_context_list"),
                JSON_OBJ_MAPPER::writeValueAsBytes,
                bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QueryGNNSamplingContext>>() {
                }));

    public static final Type<ArrayList<QuerySCCContext>> QUERY_SCC_CONTEXT_LIST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_strongly_connected_components_context_list"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QuerySCCContext>>() {
                    }));

    public static final Type<ArrayList<QueryCCContext>> QUERY_CC_CONTEXT_LIST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_connected_components_context_list"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QueryCCContext>>() {
                    }));

    public static final Type<ArrayList<QuerySSSPContext>> QUERY_SSSP_CONTEXT_LIST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_sssp_context_list"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<QuerySSSPContext>>() {
                    }));

    public static final Type<ArrayList<PageRankContext>> PAGERANK_CONTEXT_LIST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_context_list"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<PageRankContext>>() {
                    }));

    public static final Type<QueryResult> QUERY_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryResult.class));

    public static final Type<ArrayList<HistoryQuery>> QUERY_HISTORY =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "history_query"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<ArrayList<HistoryQuery>>() {
                    }));

    // list
    public static final Type<List<VertexActivity>> VERTEX_ACTIVITIES_LIST_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activities_list"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<List<VertexActivity>>() {
            }));

    // priority queue
    // red black tree TreeSort unsupported because it cannot handle duplicated timestamp
    public static final Type<PriorityQueue<VertexActivity>> VERTEX_ACTIVITIES_PQ_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activities_pq"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<PriorityQueue<VertexActivity>>() {
            }));

    // bucketed red black tree
    public static final Type<TreeMap<Integer, List<VertexActivity>>> VERTEX_ACTIVITIES_BRBT_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activities_brbt"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<TreeMap<Integer, List<VertexActivity>>>() {
            }));

    // list
    public static final Type<List<VertexActivity>> BUCKET_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "vertex_activities_bucket"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<List<VertexActivity>>() {
            }));

    public static final Type<QueryStateRequest> QUERY_STATE_REQUEST_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_state_request"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryStateRequest.class));

    public static final Type<QueryState> QUERY_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryState.class));

    public static final Type<QueryNextNeighboursInfo> QUERY_NEXT_NEIGHBOURS_INFO =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_next_neighbours_info_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryNextNeighboursInfo.class));

    public static final Type<Query> QUERY_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, Query.class));

    public static final Type<Set<String>> GRAPH_IDS_TYPE = SimpleType.simpleImmutableTypeFrom(
            TypeName.typeNameOf(TYPES_NAMESPACE, "graph_ids"),
            JSON_OBJ_MAPPER::writeValueAsBytes,
            bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<HashSet<String>>() {
            }));

    public static final Type<PageRankTask> PAGERANK_TASK_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_task"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PageRankTask.class));

    public static final Type<PageRankTaskWithState> PAGERANK_TASK_WITH_STATE_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_task_with_state"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PageRankTaskWithState.class));

    public static final Type<PageRankValueWithWeight> PAGERANK_VALUE_WITH_WEIGHT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_value_with_weight"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PageRankValueWithWeight.class));

    public static final Type<Map<Integer, Map<String, String>>> PAGERANK_RESULTS_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_results"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, new TypeReference<Map<Integer, Map<String, String>>>() {
                    }));

    public static final Type<QueryPageRankResult> QUERY_PAGERANK_RESULT_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_pagerank_result"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryPageRankResult.class));

    public static final Type<PageRankContinueTask> PAGERANK_CONTINUE_TASK_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_continue_task"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PageRankContinueTask.class));

    public static final Type<QueryGraphIds> QUERY_GRAPH_IDS_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "query_graph_ids"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, QueryGraphIds.class));

    public static final Type<ResponseGraphIds> RESPONSE_GRAPH_IDS_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "response_graph_ids"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, ResponseGraphIds.class));

    public static final Type<PageRankContextClear> PAGERANK_CONTEXT_CLEAR_TYPE =
            SimpleType.simpleImmutableTypeFrom(
                    TypeName.typeNameOf(TYPES_NAMESPACE, "pagerank_context_clear"),
                    JSON_OBJ_MAPPER::writeValueAsBytes,
                    bytes -> JSON_OBJ_MAPPER.readValue(bytes, PageRankContextClear.class));

}
