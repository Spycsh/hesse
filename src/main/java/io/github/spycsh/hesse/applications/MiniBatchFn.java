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

    // set of sources of a vertex
    /**
     * sourceId specified by the query id and user id
     */
    private static final ValueSpec<QueryMiniBatchContext> QUERY_MINI_BATCH_CONTEXT =
            ValueSpec.named("queryMiniBatchContext").withCustomType(Types.QUERY_MINI_BATCH_SOURCE_IDs_TYPE);

    private static final ValueSpec<ArrayList<QueryMiniBatchContext>> QUERY_MINI_BATCH_CONTEXT_LIST =
            ValueSpec.named("queryMiniBatchContextList").withCustomType(Types.QUERY_MINI_BATCH_CONTEXT_LIST_TYPE);


    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "mini-batch");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(MiniBatchFn::new)
            .withValueSpecs(QUERY_MINI_BATCH_CONTEXT, QUERY_MINI_BATCH_CONTEXT_LIST)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        // Secondly receive the whole log of this node
        // recover the state
        // store in current context
        // TODO not sending the whole log to recover but use checkpoint and part of log
        if(message.is(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE)){
            System.out.printf("[MiniBatchFn %s] QueryMiniBatchWithState received\n", context.self().id());

            QueryMiniBatchWithState q = message.as(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            int T = q.getT();
            ArrayList<String> neighbourIds = recoverStateAtT(context, T, vertexActivities);
            int H = q.getH();
            int K = q.getK();


            // the source of the original source of one query will be null
            ArrayList<QueryMiniBatchContext> queryMiniBatchContextList =
                    context.storage().get(QUERY_MINI_BATCH_CONTEXT_LIST).orElse(new ArrayList<>());
            // it should collect H responses (when H is less than neighbour size, collect the latter)
            int responseNumToCollect = Math.min(H, neighbourIds.size());
            queryMiniBatchContextList.add(new QueryMiniBatchContext(q.getQueryId(), q.getUserId(), responseNumToCollect, null, new ArrayList<>()));

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
                                    new ForwardQueryMiniBatch(context.self().id(), neighbourId, q, q.getK() - 1)
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
            ArrayList<String> neighbourIds = recoverStateAtT(context, T, q.getVertexActivities());
            int H = q.getH();

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
                        q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(), aggregatedResults);

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
                                    new ForwardQueryMiniBatch(context.self().id(), neighbourId, q, K - 1)
                            )
                            .build());
                }
                // find the queryMiniBatchContext with the queryId and userId
                // set the source ids and the response number
                QueryMiniBatchContext e = findQueryMiniBatchContext(q.getQueryId(), q.getUserId(), queryMiniBatchContextList);
                if(e == null){
                    QueryMiniBatchContext qc = new QueryMiniBatchContext(q.getQueryId(), q.getUserId(), Math.min(H, neighbourIds.size()),
                            new ArrayList<String>(){{add(sourceId);}}, new ArrayList<>());
                    queryMiniBatchContextList.add(qc);
                } else {
                    e.getSourceIds().add(sourceId);
                    e.setResponseNumToCollect(Math.min(H, neighbourIds.size()));
                }

                context.storage().set(QUERY_MINI_BATCH_CONTEXT_LIST, queryMiniBatchContextList);

            }
        }

        /**
         * must receive Math.min(neighbour.size), H) responses, can the vertex send the query result
         * back to its parent (source)
         */
        if (message.is(Types.QUERY_MINI_BATCH_RESULT_TYPE)){
            System.out.printf("[MiniBatchFn %s] QueryMiniBatchResult received\n", context.self().id());

            //  jackson has no idea what type of elements should be in the ArrayList object.
            // so it will parse to ArrayList<LinkedHashMap>
//            ArrayList<QueryMiniBatchResult> result = message.as(Types.QUERY_MINI_BATCH_RESULTS_TYPE);
            QueryMiniBatchResult result = message.as(Types.QUERY_MINI_BATCH_RESULT_TYPE);

            // get current context of the node to the query
            // contains the source ids of the current vertex
            // the current aggregated results
            ArrayList<QueryMiniBatchContext> queryMiniBatchContextList = context.storage().get(QUERY_MINI_BATCH_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryMiniBatchContext queryMiniBatchContext = findQueryMiniBatchContext(result.getQueryId(), result.getUserId(), queryMiniBatchContextList);

            if(queryMiniBatchContext.getResponseNumToCollect() > 1){
                System.out.printf("[MiniBatchFn %s] queryMiniBatchContext not collects all the results, still %s " +
                        " result(s) to collect\n", context.self().id(), queryMiniBatchContext.getResponseNumToCollect() - 1);

                // after collecting this result, still not collect all results,
                // so just aggregate the new result, not send or egress, because not received all the excepted results
                for(Edge e : result.getAggregatedResults()){
                    ArrayList<Edge> r = queryMiniBatchContext.getCurrentAggregatedResults();
                    r.add(e);
                }
                queryMiniBatchContext.setResponseNumToCollect(queryMiniBatchContext.getResponseNumToCollect() - 1);
            } else{
                // this is the last result to collect
                // if it is the original source, egress the aggregated results
                // otherwise, collect the last result and then collect its own results
                // and send to its the parent
                // finally delete the context of this vertex to this query
                if(queryMiniBatchContext.getSourceIds() == null){
                    System.out.printf("[MiniBatchFn %s] queryMiniBatchContext collects all the results and is the source\n",
                            context.self().id());

                    // this is the original source, egress
                    System.out.println("9999");
                    /**
                     * egress
                     */
                    System.out.println("success!!");

                    // print the last result
                    for(Edge e: result.getAggregatedResults())
                        System.out.println(e.getSrcId() + "->" + e.getDstId());

                    // print the aggregated results of the children nodes
                    for(Edge e: queryMiniBatchContext.getCurrentAggregatedResults()){
                        System.out.println(e.getSrcId() + "->" + e.getDstId());
                    }
                } else {
                    System.out.printf("[MiniBatchFn %s] queryMiniBatchContext collects all the results but not the source\n", context.self().id());

                    // for all the parent nodes, send own aggregated results
                    for(String i: queryMiniBatchContext.getSourceIds()){
                        // append received aggregated results
                        ArrayList<Edge> aggregatedResults = result.getAggregatedResults();
                        // append stored current aggregated results
                        aggregatedResults.addAll(queryMiniBatchContext.getCurrentAggregatedResults());
                        // add the new edge from parent to it self
                        aggregatedResults.add(new Edge(i, context.self().id()));
                        // result.setAggregatedResults(aggregatedResults);

                        context.send(MessageBuilder
                                .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), i)
                                .withCustomType(
                                        Types.QUERY_MINI_BATCH_RESULT_TYPE,
                                        result
                                )
                                .build());
                    }
                }

                // delete the current context of the vertex to the query
                // it is not needed anymore
                queryMiniBatchContextList.remove(queryMiniBatchContext);
            }
            context.storage().set(QUERY_MINI_BATCH_CONTEXT_LIST, queryMiniBatchContextList);
        }

        return context.done();
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

    private ArrayList<String> recoverStateAtT(Context context, int T, List<VertexActivity> activityLog){

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
}
