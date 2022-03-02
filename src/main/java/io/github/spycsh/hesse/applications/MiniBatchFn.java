package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.storage.VertexStorageFn;
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
     * this should also be specified by the query id and user id
     */
    private static final ValueSpec<HashSet<QueryMiniBatchSourceId>> QUERY_MINI_BATCH_SOURCE_IDs =
            ValueSpec.named("queryMiniBatchSourceIds").withCustomType(Types.QUERY_MINI_BATCH_SOURCE_IDs_TYPE);


    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "mini-batch");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs()
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        // Secondly receive the whole log of this node
        // recover the state
        // store in current context
        // TODO not sending the whole log to recover but use checkpoint and part of log
        if(message.is(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE)){

            QueryMiniBatchWithState q = message.as(Types.QUERY_MINI_BATCH_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            int T = q.getT();
            ArrayList<String> neighbourIds = recoverStateAtT(context, T, vertexActivities);
            int H = q.getH();
            int K = q.getK();

            // the source of the original source of one query will be null
            HashSet<QueryMiniBatchSourceId> queryMiniBatchSourceIds =
                    context.storage().get(QUERY_MINI_BATCH_SOURCE_IDs).orElse(new HashSet<>());
            queryMiniBatchSourceIds.add(new QueryMiniBatchSourceId(q.getQueryId(), q.getUserId(), null));
            context.storage().set(QUERY_MINI_BATCH_SOURCE_IDs, queryMiniBatchSourceIds);

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

            ForwardQueryMiniBatchWithState q = message.as(Types.FORWARD_QUERY_MINI_BATCH_WITH_STATE_TYPE);
            int K = q.getK();
            String sourceId = q.getSource();

            // add the source id in context
            HashSet<QueryMiniBatchSourceId> queryMiniBatchSourceIds =
                    context.storage().get(QUERY_MINI_BATCH_SOURCE_IDs).orElse(new HashSet<>());
            queryMiniBatchSourceIds.add(new QueryMiniBatchSourceId(q.getQueryId(), q.getUserId(), sourceId));
            context.storage().set(QUERY_MINI_BATCH_SOURCE_IDs, queryMiniBatchSourceIds);

            if(K == 0){
                QueryMiniBatchResult queryMiniBatchResult = new QueryMiniBatchResult(sourceId, context.self().id(),
                        q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType());
                ArrayList<QueryMiniBatchResult> queryMiniBatchResults = new ArrayList<>();
                queryMiniBatchResults.add(queryMiniBatchResult);

                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), sourceId)
                        .withCustomType(
                                Types.QUERY_MINI_BATCH_RESULTS_TYPE,
                                queryMiniBatchResults
                        )
                        .build());
            }else if(K > 0){
                int T = q.getT();
                ArrayList<String> neighbourIds = recoverStateAtT(context, T, q.getVertexActivities());
                int H = q.getH();

                // get randomized sample
                shuffle(neighbourIds);

                int sampleCnt = H;

                for (String neighbourId : neighbourIds) {
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
            }
        }

        if (message.is(Types.QUERY_MINI_BATCH_RESULTS_TYPE)){
            ArrayList<QueryMiniBatchResult> result = message.as(Types.QUERY_MINI_BATCH_RESULTS_TYPE);

            List<QueryMiniBatchSourceId> sourceIds = searchSourceIdsByQueryAndUserId(result.get(0).getQueryId(), result.get(0).getUserId(),
                    context.storage().get(QUERY_MINI_BATCH_SOURCE_IDs).orElse(new HashSet<>()));

            if(sourceIds.size() == 1 && sourceIds.get(0).getSourceId() == null){
                // egress here!
                /**
                 * egress
                 */
                System.out.println("success!!");
                for(QueryMiniBatchResult r: result)
                    System.out.println(r.getSource() + "->" + r.getTarget());

            }else{
                for(QueryMiniBatchSourceId q: sourceIds){
                    result.add(new QueryMiniBatchResult(q.getSourceId(),
                            context.self().id(),
                            result.get(0).getQueryId(),
                            result.get(0).getUserId(),
                            result.get(0).getVertexId(),
                            result.get(0).getQueryType()));

                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), q.getSourceId())
                            .withCustomType(
                                    Types.QUERY_MINI_BATCH_RESULTS_TYPE,
                                    result
                            )
                            .build());
                }
            }
        }

        return context.done();
    }

    private List<QueryMiniBatchSourceId> searchSourceIdsByQueryAndUserId(String queryId,
                                                                         String userId,
                                                                         HashSet<QueryMiniBatchSourceId> queryMiniBatchSourceIds) {
        List<QueryMiniBatchSourceId> res = new ArrayList<>();
        for(QueryMiniBatchSourceId q: queryMiniBatchSourceIds){
            if(q.getQueryId() == queryId && q.getUserId() == userId){
                res.add(q);
            }
        }
        return res;
    }

    private void shuffle(ArrayList<String> neighbourIds) {
        Collections.shuffle(neighbourIds);
    }

    private ArrayList<String> recoverStateAtT(Context context, int T, List<VertexActivity> activityLog){

        Collections.sort(activityLog, (o1, o2) -> Integer.parseInt(o2.getTimestamp()) - Integer.parseInt(o1.getTimestamp()));

        ArrayList<String> neighbourIds = new ArrayList<>();

        for(VertexActivity activity: activityLog){
            if(Integer.parseInt(activity.getTimestamp()) <= T) {  // recover the state with all the ordered activities that have event time <= T
                if (activity.getActivityType().equals("add")) {
                    // check whether has weight to decide which state to recover
                    // has weight -> weight != 0 -> HashMap<Integer, Double> a hashmap of mapping from neighbour id to weight
                    // no weight -> weight == 0 -> ArrayList<Integer> array of list of neighbour id
                    // TODO now only do with unweighted graph, the state is all the neighbours at T
                    if (Double.parseDouble(activity.getWeight()) == 0.0) {
                        neighbourIds.add(activity.getDstId());
                    }
                }
            }
        }

        return neighbourIds;
    }
}
