package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.egress.Edge;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.gnnsampling.*;
import io.github.spycsh.hesse.util.Utils;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * this function serves the temporal query of the graph about the GNNSampling,
 * i.e. K-hop H-size neighbourhood of one node in any arbitrary time window
 * refer to http://snap.stanford.edu/graphsage/sample_and_agg.png for more details
 * the question can be simplified as following:
 * given H and K and t
 * H: sample size for each hop
 * K: hop number
 * t: timestamp
 * return the K-hop H-size neighbourhood of one node
 */
public class GNNSamplingFn implements StatefulFunction {

    private static final ValueSpec<ArrayList<QueryGNNSamplingContext>> QUERY_GNN_SAMPLING_CONTEXT_LIST =
            ValueSpec.named("queryGNNSamplingContextList").withCustomType(Types.QUERY_GNN_SAMPLING_CONTEXT_LIST_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "gnn-sampling");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(GNNSamplingFn::new)
            .withValueSpecs(QUERY_GNN_SAMPLING_CONTEXT_LIST)
            .build();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(GNNSamplingFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        if(message.is(Types.QUERY_GNN_SAMPLING_WITH_STATE_TYPE)){
            LOGGER.debug("[GNNSamplingFn {}] QueryGNNSamplingWithState received", context.self().id());

            QueryGNNSamplingState q = message.as(Types.QUERY_GNN_SAMPLING_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();

            HashSet<String> neighbourIds = Utils.recoverStateByLog(vertexActivities);

            // if there are no outer edges, directly egress
            if(neighbourIds.size() == 0){
                StringBuilder result = new StringBuilder();
                String str1 = String.format("Result of query %s by user %s: no results found for node %s because it is isolated!",
                        q.getQueryId(), q.getUserId(), context.self().id());
                result.append(str1);
                sendResult(context, q.getQueryId(), q.getUserId(), result.toString());
            }else {

                int H = q.getH();
                int K = q.getK();

                // the source of the original source of one query will be null
                ArrayList<QueryGNNSamplingContext> queryGNNSamplingContextList =
                        context.storage().get(QUERY_GNN_SAMPLING_CONTEXT_LIST).orElse(new ArrayList<>());


                ArrayDeque<String> firstStk = new ArrayDeque<String>() {{
                    add(context.self().id());
                }};

                // it should collect H responses (when H is less than neighbour size, collect the neighbour size)
                int responseNumToCollect = Math.min(H, neighbourIds.size());

                GNNSamplingPathContext GNNSamplingPathContext = new GNNSamplingPathContext(Utils.generateNewStackHash(firstStk), responseNumToCollect, new ArrayList<>());
                queryGNNSamplingContextList.add(new QueryGNNSamplingContext(q.getQueryId(), q.getUserId(), new ArrayList<GNNSamplingPathContext>() {{
                    add(GNNSamplingPathContext);
                }}));

                context.storage().set(QUERY_GNN_SAMPLING_CONTEXT_LIST, queryGNNSamplingContextList);

                // get randomized sample
                ArrayList<String> neighbourIdList = new ArrayList<>(neighbourIds);
                shuffle(neighbourIdList);
                if (K > 0) {
                    int sampleCnt = H;
                    for (String neighbourId : neighbourIdList) {
                        if (sampleCnt <= 0) break;  // get H sample
                        sampleCnt -= 1;

                        context.send(MessageBuilder
                                .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                                .withCustomType(
                                        Types.FORWARD_QUERY_GNN_SAMPLING_TYPE,
                                        new ForwardQueryGNNSampling(context.self().id(), neighbourId, q, q.getK() - 1, firstStk)
                                )
                                .build());
                    }
                }
            }
        }

        if(message.is(Types.FORWARD_QUERY_GNN_SAMPLING_WITH_STATE_TYPE)){
            LOGGER.debug("[GNNSamplingFn {}] ForwardQueryGNNSamplingWithState received", context.self().id());

            ForwardQueryGNNSamplingWithState q = message.as(Types.FORWARD_QUERY_GNN_SAMPLING_WITH_STATE_TYPE);
            int K = q.getK();
            String sourceId = q.getSource();

            ArrayList<QueryGNNSamplingContext> queryGNNSamplingContextList = context.storage().get(QUERY_GNN_SAMPLING_CONTEXT_LIST).orElse(new ArrayList<>());

            HashSet<String> neighbourIds = Utils.recoverStateByLog(q.getVertexActivities());
            int H = q.getH();

            ArrayDeque<String> stack = q.getStack();

            if(K == 0 || neighbourIds.size() == 0){
                // if already is the last hop or the vertex has no more neighbours
                // do not need to set source ids, just send back the result
                if(K==0){
                    LOGGER.debug("[GNNSamplingFn {}] ForwardQueryGNNSamplingWithState K is 0, send back the queryGNNSamplingResult", context.self().id());
                }else{
                    LOGGER.debug("[GNNSamplingFn {}] ForwardQueryGNNSamplingWithState neighbour size is 0, send back the queryGNNSamplingResult", context.self().id());
                }

                ArrayList<Edge> aggregatedResults = new ArrayList<>();
                aggregatedResults.add(new Edge(sourceId, context.self().id()));
                QueryGNNSamplingResult queryGNNSamplingResult = new QueryGNNSamplingResult(
                        q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(), aggregatedResults, stack);

                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "gnn-sampling"), sourceId)
                        .withCustomType(
                                Types.QUERY_GNN_SAMPLING_RESULT_TYPE,
                                queryGNNSamplingResult
                        )
                        .build());
            }else if(K > 0){
                LOGGER.debug("[GNNSamplingFn {}] ForwardQueryGNNSamplingWithState K > 0, continue forwarding to neighbours",
                        context.self().id());

                // get randomized sample
                ArrayList<String> neighbourIdList = new ArrayList<>(neighbourIds);
                shuffle(neighbourIdList);

                stack.addFirst(context.self().id());

                int sampleCnt = H;

                for (String neighbourId : neighbourIdList) {

                    LOGGER.debug("[GNNSamplingFn {}] forwarding to neighbour {}...", context.self().id(), neighbourId);
                    if (sampleCnt <= 0) break;
                    sampleCnt -= 1;
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_GNN_SAMPLING_TYPE,
                                    new ForwardQueryGNNSampling(context.self().id(), neighbourId, q, K - 1, stack)
                            )
                            .build());
                }
                // find the queryGNNSamplingContext with the queryId and userId
                // set the source ids and the response number
                QueryGNNSamplingContext e = findQueryGNNSamplingContext(q.getQueryId(), q.getUserId(), queryGNNSamplingContextList);
                ArrayDeque<String> currentStack = q.getStack();
                int responseNumToCollect = Math.min(H, neighbourIds.size());

                if(e == null){
                    ArrayList<GNNSamplingPathContext> list = new ArrayList<>();

                    list.add(new GNNSamplingPathContext(Utils.generateNewStackHash(currentStack), responseNumToCollect, new ArrayList<>()));
                    QueryGNNSamplingContext qc = new QueryGNNSamplingContext(q.getQueryId(), q.getUserId(),
                            list);
                    queryGNNSamplingContextList.add(qc);
                } else {
                    e.getGNNSamplingPathContexts().add(
                            new GNNSamplingPathContext(Utils.generateNewStackHash(currentStack),
                                responseNumToCollect,
                                new ArrayList<>()));
                }

                context.storage().set(QUERY_GNN_SAMPLING_CONTEXT_LIST, queryGNNSamplingContextList);

            }
        }

        // must receive Math.min(neighbour.size, H) responses, can the vertex send the query result
        // back to its parent (source)
        if (message.is(Types.QUERY_GNN_SAMPLING_RESULT_TYPE)){
            LOGGER.debug("[GNNSamplingFn {}}] QueryGNNSamplingResult received", context.self().id());

            // jackson has no idea what type of elements should be in the ArrayList object.
            // so it will parse to ArrayList<LinkedHashMap>
            QueryGNNSamplingResult result = message.as(Types.QUERY_GNN_SAMPLING_RESULT_TYPE);

            // get current context of the node to the query
            ArrayList<QueryGNNSamplingContext> queryGNNSamplingContextList = context.storage().get(QUERY_GNN_SAMPLING_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryGNNSamplingContext queryGNNSamplingContext = findQueryGNNSamplingContext(result.getQueryId(), result.getUserId(), queryGNNSamplingContextList);

            ArrayDeque<String> stack = result.getStack();
            int stackHash = Utils.generateNewStackHash(stack);

            // find in context the current response num to collect by stackHash
            if(queryGNNSamplingContext == null){
                LOGGER.error("queryGNNSamplingContext should not be null!");
                throw new IllegalStateException("queryGNNSamplingContext should not be null!");
            }
            ArrayList<GNNSamplingPathContext> GNNSamplingPathContext = queryGNNSamplingContext.getGNNSamplingPathContexts();
            GNNSamplingPathContext gnnSamplingContextByPathHash = findGNNSamplingContextByStackHash(GNNSamplingPathContext, stackHash);

            stack.removeFirst();

            if(gnnSamplingContextByPathHash == null){
                LOGGER.error("gnnSamplingContextByPathHash should not be null!");
                throw new IllegalStateException("gnnSamplingContextByPathHash should not be null!");
            }
            if(gnnSamplingContextByPathHash.getResponseNum() > 1){
                LOGGER.debug("[GNNSamplingFn {}] queryGNNSamplingContext not collects all the results, still {} " +
                        " result(s) to collect", context.self().id(), gnnSamplingContextByPathHash.getResponseNum() - 1);

                // after collecting this result, still not collect all results,
                // so just aggregate the new result, not send or egress, because not received all the excepted results
                for(Edge e : result.getAggregatedResults()){
                    List<Edge> r = gnnSamplingContextByPathHash.getAggregatedGNNSamplingEdges();
                    r.add(e);
                }

                gnnSamplingContextByPathHash.setResponseNum(gnnSamplingContextByPathHash.getResponseNum() - 1);
            } else {
                // this is the last result to collect
                // if it is the original source, egress the aggregated results
                // otherwise, collect the last result and then collect its own results
                // and send to its the parent
                // finally delete the context of this vertex to this query
                if(context.self().id().equals(result.getVertexId())){
                    // queryGNNSamplingContext collects all the results and is the source
                    LOGGER.info("[GNNSamplingFn {}] Success! qid: {}, uid: {}",
                            context.self().id(), result.getQueryId(), result.getUserId());

                    result.getAggregatedResults().addAll(gnnSamplingContextByPathHash.getAggregatedGNNSamplingEdges());

                    StringBuilder sb = new StringBuilder();
                    sb.append(String.format("Result of query %s by user %s, edges including in the gnn-sampling are: ", result.getQueryId(), result.getUserId()));
                    for(Edge e: result.getAggregatedResults())
                        sb.append(e.getSrcId()).append("->").append(e.getDstId()).append(" ");

                    sendResult(context, result.getQueryId(), result.getUserId(), sb.toString());

                } else {
                    LOGGER.debug("[GNNSamplingFn {}] queryGNNSamplingContext collects all the results but not the source", context.self().id());

                    // add the buffered gnn sampling results to the result and sent back to its parent node
                    result.getAggregatedResults().addAll(gnnSamplingContextByPathHash.getAggregatedGNNSamplingEdges());

                    // and also add the edge from self to the parent node
                    result.getAggregatedResults().add(new Edge(stack.getFirst(), context.self().id()));

                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "gnn-sampling"), stack.getFirst())
                            .withCustomType(
                                    Types.QUERY_GNN_SAMPLING_RESULT_TYPE,
                                    result
                            )
                            .build());

                }

                // delete the current context of the vertex to the query
                // it is not needed anymore
                queryGNNSamplingContext.getGNNSamplingPathContexts().remove(gnnSamplingContextByPathHash);
                if(queryGNNSamplingContext.getGNNSamplingPathContexts().size() == 0)
                    queryGNNSamplingContextList.remove(queryGNNSamplingContext);
            }
            context.storage().set(QUERY_GNN_SAMPLING_CONTEXT_LIST, queryGNNSamplingContextList);
        }

        return context.done();
    }

    private GNNSamplingPathContext findGNNSamplingContextByStackHash(ArrayList<GNNSamplingPathContext> GNNSamplingPathContext, int stackHash) {
        for(GNNSamplingPathContext e: GNNSamplingPathContext){
            if(e.getPathHash() == stackHash){
                return e;
            }
        }
        return null;
    }

    private QueryGNNSamplingContext findQueryGNNSamplingContext(String queryId, String userId, ArrayList<QueryGNNSamplingContext> queryGNNSamplingContextList) {
        for(QueryGNNSamplingContext e: queryGNNSamplingContextList) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }

    private void shuffle(ArrayList<String> neighbourIds) {
        Collections.shuffle(neighbourIds);
    }

    private void sendResult(Context context, String queryId, String userId, String resultStr) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.query", "temporal-query-handler"), queryId)
                .withCustomType(
                        Types.QUERY_RESULT_TYPE,
                        new QueryResult(queryId, userId, resultStr))
                .build());
    }

}
