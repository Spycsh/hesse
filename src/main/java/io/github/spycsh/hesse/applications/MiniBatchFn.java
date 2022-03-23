package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * given H and K and t
 * H: sample size for each hop
 * K: hop number
 * t: timestamp
 * return the K-hop H-size neighbourhood of one node
 */
public class MiniBatchFn implements StatefulFunction {

    private static final ValueSpec<ArrayList<QueryMiniBatchContext>> QUERY_MINI_BATCH_CONTEXT_LIST =
            ValueSpec.named("queryMiniBatchContextList").withCustomType(Types.QUERY_MINI_BATCH_CONTEXT_LIST_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "mini-batch");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(MiniBatchFn::new)
            .withValueSpecs(QUERY_MINI_BATCH_CONTEXT_LIST)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        if(message.is(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE)){
            System.out.printf("[MiniBatchFn %s] QueryMiniBatchWithState received\n", context.self().id());

            QueryMiniBatchWithState q = message.as(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            int T = q.getT();
            ArrayList<String> neighbourIds = recoverStateAtT(T, vertexActivities);
            int H = q.getH();
            int K = q.getK();

            // the source of the original source of one query will be null
            ArrayList<QueryMiniBatchContext> queryMiniBatchContextList =
                    context.storage().get(QUERY_MINI_BATCH_CONTEXT_LIST).orElse(new ArrayList<>());


            ArrayDeque<String> firstStk = new ArrayDeque<String>() {{
                add(context.self().id());
            }};
            /**
             * the first node only need to receive min(H, neighbourIds.size()) results
             */
            // it should collect H responses (when H is less than neighbour size, collect the neighbour size)
            int responseNumToCollect = Math.min(H, neighbourIds.size());

            MiniBatchPathContext miniBatchPathContext = new MiniBatchPathContext(generateNewStackHash(firstStk), responseNumToCollect, new ArrayList<Edge>());
            queryMiniBatchContextList.add(new QueryMiniBatchContext(q.getQueryId(), q.getUserId(), new ArrayList<MiniBatchPathContext>(){{
                add(miniBatchPathContext);
            }}));

            context.storage().set(QUERY_MINI_BATCH_CONTEXT_LIST, queryMiniBatchContextList);

            // get randomized sample
            shuffle(neighbourIds);
            if(K > 0) {
                int sampleCnt = H;
                for (String neighbourId : neighbourIds) {
                    if (sampleCnt <= 0) break;  // get H sample
                    sampleCnt -= 1;

                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_MINI_BATCH_TYPE,
                                    new ForwardQueryMiniBatch(context.self().id(), neighbourId, q, q.getK() - 1, firstStk)
                            )
                            .build());
                }
            }
        } else if(message.is(Types.FORWARD_QUERY_MINI_BATCH_WITH_STATE_TYPE)){
            System.out.printf("[MiniBatchFn %s] ForwardQueryMiniBatchWithState received\n", context.self().id());

            ForwardQueryMiniBatchWithState q = message.as(Types.FORWARD_QUERY_MINI_BATCH_WITH_STATE_TYPE);
            int K = q.getK();
            String sourceId = q.getSource();

            ArrayList<QueryMiniBatchContext> queryMiniBatchContextList = context.storage().get(QUERY_MINI_BATCH_CONTEXT_LIST).orElse(new ArrayList<>());

            int T = q.getT();
            ArrayList<String> neighbourIds = recoverStateAtT(T, q.getVertexActivities());
            int H = q.getH();

            ArrayDeque<String> stack = q.getStack();

            if(K == 0 || neighbourIds.size() == 0){
                // if already is the last hop or the vertex has no more neighbours
                // do not need to set source ids, just send back the result
                if(K==0){
                    System.out.printf("[MiniBatchFn %s] ForwardQueryMiniBatchWithState K is 0, send back the queryMiniBatchResult\n",
                            context.self().id());
                }else{
                    System.out.printf("[MiniBatchFn %s] ForwardQueryMiniBatchWithState neighbour size is 0, send back the queryMiniBatchResult\n",
                            context.self().id());
                }


                ArrayList<Edge> aggregatedResults = new ArrayList<>();
                aggregatedResults.add(new Edge(sourceId, context.self().id()));
                QueryMiniBatchResult queryMiniBatchResult = new QueryMiniBatchResult(
                        q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(), aggregatedResults, stack);

                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), sourceId)
                        .withCustomType(
                                Types.QUERY_MINI_BATCH_RESULT_TYPE,
                                queryMiniBatchResult
                        )
                        .build());
            }else if(K > 0){

                System.out.printf("[MiniBatchFn %s] ForwardQueryMiniBatchWithState K > 0, continue forwarding to neighbours\n",
                        context.self().id());

                // get randomized sample
                shuffle(neighbourIds);

                stack.addFirst(context.self().id());

                int sampleCnt = H;

                for (String neighbourId : neighbourIds) {
                    System.out.printf("[MiniBatchFn %s] forwarding to neighbour %s... \n",
                            context.self().id(), neighbourId);
                    if (sampleCnt <= 0) break;
                    sampleCnt -= 1;
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_MINI_BATCH_TYPE,
                                    new ForwardQueryMiniBatch(context.self().id(), neighbourId, q, K - 1, stack)
                            )
                            .build());
                }
                // find the queryMiniBatchContext with the queryId and userId
                // set the source ids and the response number
                QueryMiniBatchContext e = findQueryMiniBatchContext(q.getQueryId(), q.getUserId(), queryMiniBatchContextList);
                ArrayDeque<String> currentStack = q.getStack();
                int responseNumToCollect = Math.min(H, neighbourIds.size());

                if(e == null){
                    ArrayList<MiniBatchPathContext> list = new ArrayList<>();

                    list.add(new MiniBatchPathContext(generateNewStackHash(currentStack), responseNumToCollect, new ArrayList<>()));
                    QueryMiniBatchContext qc = new QueryMiniBatchContext(q.getQueryId(), q.getUserId(),
                            list);
                    queryMiniBatchContextList.add(qc);
                } else {
                    e.getMiniBatchPathContexts().add(
                            new MiniBatchPathContext(generateNewStackHash(currentStack),
                                responseNumToCollect,
                                new ArrayList<>()));
                }

                context.storage().set(QUERY_MINI_BATCH_CONTEXT_LIST, queryMiniBatchContextList);

            }
        }

        /**
         * must receive Math.min(neighbour.size, H) responses, can the vertex send the query result
         * back to its parent (source)
         */
        if (message.is(Types.QUERY_MINI_BATCH_RESULT_TYPE)){
            System.out.printf("[MiniBatchFn %s] QueryMiniBatchResult received\n", context.self().id());

            //  jackson has no idea what type of elements should be in the ArrayList object.
            // so it will parse to ArrayList<LinkedHashMap>
//            ArrayList<QueryMiniBatchResult> result = message.as(Types.QUERY_MINI_BATCH_RESULTS_TYPE);
            QueryMiniBatchResult result = message.as(Types.QUERY_MINI_BATCH_RESULT_TYPE);

            // get current context of the node to the query
            ArrayList<QueryMiniBatchContext> queryMiniBatchContextList = context.storage().get(QUERY_MINI_BATCH_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryMiniBatchContext queryMiniBatchContext = findQueryMiniBatchContext(result.getQueryId(), result.getUserId(), queryMiniBatchContextList);

            ArrayDeque<String> stack = result.getStack();
            int stackHash = generateNewStackHash(stack);

            // find in context the current response num to collect by stackHash
            ArrayList<MiniBatchPathContext> miniBatchPathContext = queryMiniBatchContext.getMiniBatchPathContexts();
            MiniBatchPathContext miniBatchContextByPathHash = findMiniBatchContextByStackHash(miniBatchPathContext, stackHash);

            stack.removeFirst();

            if(miniBatchContextByPathHash.getResponseNum() > 1){
                System.out.printf("[MiniBatchFn %s] queryMiniBatchContext not collects all the results, still %s " +
                        " result(s) to collect\n", context.self().id(), miniBatchContextByPathHash.getResponseNum() - 1);

                // after collecting this result, still not collect all results,
                // so just aggregate the new result, not send or egress, because not received all the excepted results
                for(Edge e : result.getAggregatedResults()){
                    List<Edge> r = miniBatchContextByPathHash.getAggregatedMiniBatchEdges();
                    r.add(e);
                }

                miniBatchContextByPathHash.setResponseNum(miniBatchContextByPathHash.getResponseNum() - 1);
            } else {
                // this is the last result to collect
                // if it is the original source, egress the aggregated results
                // otherwise, collect the last result and then collect its own results
                // and send to its the parent
                // finally delete the context of this vertex to this query
                if(context.self().id().equals(result.getVertexId())){
                    System.out.printf("[MiniBatchFn %s] queryMiniBatchContext collects all the results and is the source\n",
                            context.self().id());

                    // this is the original source, egress
                    System.out.println("9999");
                    /**
                     * egress
                     */
                    System.out.println("success!!");

                    result.getAggregatedResults().addAll(miniBatchContextByPathHash.getAggregatedMiniBatchEdges());

                    // print the last result
                    for(Edge e: result.getAggregatedResults())
                        System.out.println(e.getSrcId() + "->" + e.getDstId());

                    // print the aggregated results of the children nodes
//                    for(Edge e: miniBatchContextByPathHash.getAggregatedMiniBatchEdges()){
//                        System.out.println(e.getSrcId() + "->" + e.getDstId());
//                    }
                } else {
                    System.out.printf("[MiniBatchFn %s] queryMiniBatchContext collects all the results but not the source\n", context.self().id());

                    // add the buffered minibatch results to the result and sent back to its parent node
                    result.getAggregatedResults().addAll(miniBatchContextByPathHash.getAggregatedMiniBatchEdges());

                    // and also add the edge from self to the parent node
                    result.getAggregatedResults().add(new Edge(stack.getFirst(), context.self().id()));

                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), stack.getFirst())
                            .withCustomType(
                                    Types.QUERY_MINI_BATCH_RESULT_TYPE,
                                    result
                            )
                            .build());

                }

                // delete the current context of the vertex to the query
                // it is not needed anymore
                queryMiniBatchContext.getMiniBatchPathContexts().remove(miniBatchContextByPathHash);
                if(queryMiniBatchContext.getMiniBatchPathContexts().size() == 0)
                    queryMiniBatchContextList.remove(queryMiniBatchContext);
            }
            context.storage().set(QUERY_MINI_BATCH_CONTEXT_LIST, queryMiniBatchContextList);
        }

        return context.done();
    }

    private MiniBatchPathContext findMiniBatchContextByStackHash(ArrayList<MiniBatchPathContext> miniBatchPathContext, int stackHash) {
        for(MiniBatchPathContext e: miniBatchPathContext){
            if(e.getPathHash() == stackHash){
                return e;
            }
        }
        return null;
    }

    private QueryMiniBatchContext findQueryMiniBatchContext(String queryId, String userId, ArrayList<QueryMiniBatchContext> queryMiniBatchContextList) {
        for(QueryMiniBatchContext e: queryMiniBatchContextList) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }

    private void shuffle(ArrayList<String> neighbourIds) {
        Collections.shuffle(neighbourIds);
    }

    private ArrayList<String> recoverStateAtT(int T, List<VertexActivity> activityLog){

        activityLog.sort((o1, o2) -> Integer.parseInt(o2.getTimestamp()) - Integer.parseInt(o1.getTimestamp()));

        ArrayList<String> neighbourIds = new ArrayList<>();

        for(VertexActivity activity: activityLog){
            if(Integer.parseInt(activity.getTimestamp()) <= T) {  // recover the state with all the ordered activities that have event time <= T
                if (activity.getActivityType().equals("add")) {
                    // check whether has weight to decide which state to recover
                    // has weight -> weight != 0 -> HashMap<Integer, Double> a hashmap of mapping from neighbour id to weight
                    // no weight -> weight == 0 -> ArrayList<Integer> array of list of neighbour id
                    // TODO now only do with unweighted graph, the state is all the neighbours at T
                    if (activity.getWeight() == null) {
                        neighbourIds.add(activity.getDstId());
                    }
                }
            }
        }

        return neighbourIds;
    }

    private int generateNewStackHash(ArrayDeque<String> stack) {
        StringBuffer sb = new StringBuffer();
        for(String s:stack){
            sb.append(s).append(" ");
        }
        return sb.toString().hashCode();
    }
}
