package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.cc.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * this function serves the temporal query of the vertex's connected component id
 * and all ids in the same connected component in any arbitrary time window
 */
public class ConnectedComponentsFn implements StatefulFunction {

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "connected-components");
    private static final ValueSpec<ArrayList<QueryCCContext>> QUERY_CC_CONTEXT_LIST =
            ValueSpec.named("queryCCContext").withCustomType(Types.QUERY_CC_CONTEXT_LIST_TYPE);
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(ConnectedComponentsFn::new)
            .withValueSpecs(QUERY_CC_CONTEXT_LIST)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.QUERY_CC_WITH_STATE_TYPE)){
            System.out.printf("[ConnectedComponentsFn %s] QueryCCWithState received\n", context.self().id());

            QueryCCWithState q = message.as(Types.QUERY_CC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            int startT = q.getStartT();
            int endT = q.getEndT();
            ArrayList<String> neighbourIds = recoverStateByTimeRegion(startT, endT, vertexActivities);

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            ArrayDeque<String> firstStk = new ArrayDeque<String>() {{
                add(context.self().id());
            }};
            CCPathContext ccPathContext = new CCPathContext(generateNewStackHash(firstStk), context.self().id(), neighbourIds.size(), new HashSet<>());
            queryCCContexts.add(new QueryCCContext(q.getQueryId(), q.getUserId(),
                    new ArrayList<CCPathContext>(){{add(ccPathContext);}}));

            ArrayDeque<String> stack = new ArrayDeque<>();
            stack.addFirst(q.getVertexId());    // add itself in the stack
            for(String neighbourId:neighbourIds){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                        .withCustomType(
                                Types.FORWARD_QUERY_CC_TYPE,
                                new ForwardQueryCC(context.self().id(), neighbourId, stack, q)
                        )
                        .build());
            }

            context.storage().set(QUERY_CC_CONTEXT_LIST, queryCCContexts);
        }

        if(message.is(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE)){
            System.out.printf("[ConnectedComponentsFn %s] ForwardQueryCCWithState received\n", context.self().id());

            ForwardQueryCCWithState q = message.as(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            ArrayList<String> neighbourIds = recoverStateByTimeRegion(q.getStartT(), q.getEndT(), vertexActivities);

            ArrayDeque<String> stack = q.getStack();

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryCCContext queryCCContext = findCCContext(q.getQueryId(), q.getUserId(), queryCCContexts);

            // if self already on stack path or has no more neighbours, then return response
            // otherwise, continue forwarding
            if(stack.contains(context.self().id()) || neighbourIds.size() == 0){
                System.out.printf("[ConnectedComponentsFn %s] ForwardQueryCCWithState received and self" +
                        " is already on the stack or has no more neighbours\n", context.self().id());

                Set<String> aggregatedCCIds = new HashSet<>();
                if(neighbourIds.size() == 0)
                    aggregatedCCIds.add(context.self().id());
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "connected-components"), q.getSource())
                        .withCustomType(
                                Types.QUERY_CC_RESULT_TYPE,
                                new QueryCCResult(q.getQueryId(), q.getUserId(),
                                        q.getVertexId(), q.getQueryType(), context.self().id(), aggregatedCCIds, stack)
                        )
                        .build());

            } else {
                // continue forwarding and also keep the path context in self
                System.out.printf("[ConnectedComponentsFn %s] ForwardQueryCCWithState received and self" +
                        " is not on the stack, continue forwarding\n", context.self().id());

                stack.addFirst(context.self().id());    // add itself in the stack
                int newStackHash = generateNewStackHash(stack);

                if(queryCCContext == null){ // first path context
                    CCPathContext ccPathContext = new CCPathContext(newStackHash, context.self().id(),  neighbourIds.size(), new HashSet<>());
                    queryCCContext = new QueryCCContext(q.getQueryId(), q.getUserId(), new ArrayList<CCPathContext>(){{add(ccPathContext);}});
                    queryCCContexts.add(queryCCContext);
                } else {
                    ArrayList<CCPathContext> ccPathContexts = queryCCContext.getCcPathContexts();
                    ccPathContexts.add(new CCPathContext(newStackHash, context.self().id(), neighbourIds.size(), new HashSet<>()));
                }

                context.storage().set(QUERY_CC_CONTEXT_LIST, queryCCContexts);

                for(String neighbourId:neighbourIds){
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_CC_TYPE,
                                    new ForwardQueryCC(context.self().id(), neighbourId, stack, q)
                            )
                            .build());
                }
            }
        }

        if(message.is(Types.QUERY_CC_RESULT_TYPE)){
            System.out.printf("[ConnectedComponentsFn %s] QueryCCResult received\n", context.self().id());
            QueryCCResult result = message.as(Types.QUERY_CC_RESULT_TYPE);

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryCCContext queryCCContext = findCCContext(result.getQueryId(), result.getUserId(), queryCCContexts);
            if(queryCCContext == null){
                throw new IllegalStateException("queryCCContext should not be null because the result is sent back from children nodes\n");
            }

            ArrayDeque<String> stack = result.getStack();
            int stackHash = generateNewStackHash(stack);

            // find in context the current response num to collect by stackHash
            ArrayList<CCPathContext> ccPathContexts = queryCCContext.getCcPathContexts();
            CCPathContext ccContextByPathHash = findCCContextByPathHash(ccPathContexts, stackHash);

            // add all the aggregated cc ids for one path from descendants
            if(ccContextByPathHash == null){
                throw new IllegalStateException("ccContextByPathHash should not be null!");
            }
            ccContextByPathHash.getAggregatedCCIds().addAll(result.getAggregatedCCIds());

            int n = ccContextByPathHash.getResponseNum();

            // remove the first of the stack namely itself
            stack.removeFirst();

            // update the low link id and store into context
            int updatedLowLinkId = Integer.parseInt(ccContextByPathHash.getAggregatedLowLinkId());
            updatedLowLinkId = Math.min(Integer.parseInt(result.getLowLinkId()), updatedLowLinkId);
            ccContextByPathHash.setAggregatedLowLinkId(String.valueOf(updatedLowLinkId));

            // all responses are collected for one path
            if(n - 1 == 0){
                System.out.printf("[ConnectedComponentsFn %s] QueryCCResult received " +
                        "and all responses are collected for one path\n", context.self().id());

                // if it is the source node, just egress
                if(context.self().id().equals(result.getVertexId())){
                    System.out.printf("[ConnectedComponentsFn %s] is source node, success!\n", context.self().id());
                    System.out.printf("[ConnectedComponentsFn %s] Result of query %s by user %s: connected component id of node %s is %s \n",
                            context.self().id(), result.getQueryId(), result.getUserId(), context.self().id(), updatedLowLinkId);
                    System.out.printf("[ConnectedComponentsFn %s] Other node ids that contain in the same component are: ",
                            context.self().id());
                    for(String id : ccContextByPathHash.getAggregatedCCIds())
                        System.out.print(id + " ");
                    System.out.println();
                } else {
                    // not source node, send to its parents the aggregated low link id
                    System.out.printf("[ConnectedComponentsFn %s] not the source node\n", context.self().id());

                    // merge all the cc ids, send to parent
                    Set<String> aggregatedCCIds = new HashSet<>();
                    for(CCPathContext c: ccPathContexts){
                        aggregatedCCIds.addAll(c.getAggregatedCCIds());
                    }
                    aggregatedCCIds.add(context.self().id());

                    // backtracking
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "connected-components"), stack.getFirst())
                            .withCustomType(
                                    Types.QUERY_CC_RESULT_TYPE,
                                    new QueryCCResult(result.getQueryId(), result.getUserId(),
                                            result.getVertexId(), result.getQueryType(), String.valueOf(updatedLowLinkId), aggregatedCCIds, stack)
                            )
                            .build());
                }

                // no longer need the CC context of current node
                // remove the pathContext using stackHash
                // remove this path
                queryCCContext.getCcPathContexts().remove(ccContextByPathHash);
                if(queryCCContext.getCcPathContexts().size() == 0){
                    // if no paths
                    // no longer need the CC context of current node
                    queryCCContexts.remove(queryCCContext);
                }

            } else {  // not the last result to collect
                System.out.printf("[ConnectedComponentsFn %s] QueryCCResult received " +
                        "but not all responses of one path are collected\n", context.self().id());
                ccContextByPathHash.setResponseNum(n - 1);
            }
            context.storage().set(QUERY_CC_CONTEXT_LIST, queryCCContexts);
        }

        return context.done();
    }

    private ArrayList<String> recoverStateByTimeRegion(int startT, int endT, List<VertexActivity> activityLog) {
        activityLog.sort((o1, o2) -> Integer.parseInt(o2.getTimestamp()) - Integer.parseInt(o1.getTimestamp()));

        ArrayList<String> neighbourIds = new ArrayList<>();

        for(VertexActivity activity: activityLog){
            // recover the state with all the ordered activities between startT and endT
            if(Integer.parseInt(activity.getTimestamp()) >= startT && Integer.parseInt(activity.getTimestamp()) <= endT) {
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
        StringBuilder sb = new StringBuilder();
        for(String s:stack){
            sb.append(s).append(" ");
        }
        return sb.toString().hashCode();
    }

    private QueryCCContext findCCContext(String queryId, String userId, ArrayList<QueryCCContext> list) {
        for(QueryCCContext e: list) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }

    private CCPathContext findCCContextByPathHash(ArrayList<CCPathContext> ccPathContexts, int stackHash) {
        for(CCPathContext c:ccPathContexts){
            if(c.getPathHash() == stackHash){
                return c;
            }
        }
        return null;
    }
}
