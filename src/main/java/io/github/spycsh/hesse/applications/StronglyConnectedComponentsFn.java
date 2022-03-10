package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class StronglyConnectedComponentsFn implements StatefulFunction {

    /**
     * sourceId specified by the query id and user id
     */
    private static final ValueSpec<ArrayList<QuerySCCContext>> QUERY_SCC_CONTEXT_LIST =
            ValueSpec.named("querySCCContext").withCustomType(Types.QUERY_SCC_CONTEXT_LIST_TYPE);


    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "strongly-connected-components");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(StronglyConnectedComponentsFn::new)
            .withValueSpecs(QUERY_SCC_CONTEXT_LIST)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.QUERY_SCC_WITH_STATE_TYPE)){
            System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCWithState received\n", context.self().id());

            QuerySCCWithState q = message.as(Types.QUERY_SCC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            int T = q.getT();
            ArrayList<String> neighbourIds = recoverStateAtT(T, vertexActivities);

            ArrayList<QuerySCCContext> querySCCContexts = context.storage().get(QUERY_SCC_CONTEXT_LIST).orElse(new ArrayList<>());
            querySCCContexts.add(new QuerySCCContext(q.getQueryId(), q.getUserId(), neighbourIds.size(), context.self().id(), null, false));

            ArrayDeque<String> stack = new ArrayDeque<>();
            stack.addFirst(q.getVertexId());    // add itself in the stack
            for(String neighbourId:neighbourIds){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                        .withCustomType(
                                Types.FORWARD_QUERY_SCC_TYPE,
                                new ForwardQuerySCC(context.self().id(), neighbourId, stack, q)
                        )
                        .build());
            }
        }

        if(message.is(Types.FORWARD_QUERY_SCC_WITH_STATE_TYPE)){
            System.out.printf("[StronglyConnectedComponentsFn %s] ForwardQuerySCCWithState received\n", context.self().id());

            ForwardQuerySCCWithState q = message.as(Types.FORWARD_QUERY_SCC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            ArrayList<String> neighbourIds = recoverStateAtT(q.getT(), vertexActivities);

            ArrayDeque<String> stack = q.getStack();

            ArrayList<QuerySCCContext> querySCCContexts = context.storage().get(QUERY_SCC_CONTEXT_LIST).orElse(new ArrayList<>());
            QuerySCCContext querySCCContext = findSCCContext(q.getQueryId(), q.getUserId(), querySCCContexts);

            // if there is a cycle back to the queried vertex, then there is a strongly connected component
            if(q.getQueryId().equals(context.self().id())){
                // if there exists a path back to the original source
                // start to aggregate low link values returned back to the original source
                if(querySCCContext == null){
                    throw new IllegalStateException("querySCCContext should not be null because it is on the stack");
                } else {
                    for(String src: querySCCContext.getSourceIds()){
                        context.send(MessageBuilder
                                .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), src)
                                .withCustomType(
                                        Types.QUERY_SCC_RESULT_TYPE,
                                        new QuerySCCResult(q.getQueryId(), q.getUserId(),
                                                q.getVertexId(), q.getQueryType(), context.self().id(), true)
                                )
                                .build());
                    }
                }

            }else if(stack.contains(context.self().id()) || neighbourIds.size() == 0){
                // if there exists a cycle but not to the original source, or there are no more neighbours
                // remove them from the stack
                // namely send QuerySCCResult with the scc flag false
                if(querySCCContext == null){
                    throw new IllegalStateException("querySCCContext should not be null because it is on the stack");
                } else{
                    for(String src: querySCCContext.getSourceIds()){
                        context.send(MessageBuilder
                                .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), src)
                                .withCustomType(
                                        Types.QUERY_SCC_RESULT_TYPE,
                                        new QuerySCCResult(q.getQueryId(), q.getUserId(),
                                                q.getVertexId(), q.getQueryType(), context.self().id(), false)
                                )
                                .build());
                    }
                }
            }else{
                // if it has not been on the stack
                // pushes self into the stack and continues to forward to neighbours
                if(querySCCContext == null){
                    querySCCContexts.add(new QuerySCCContext(q.getQueryId(), q.getUserId(), neighbourIds.size(), context.self().id(), new ArrayList<String>(){{add(q.getSource());}}, false));
                } else {
                    querySCCContext.getSourceIds().add(q.getSource());
                }
                context.storage().set(QUERY_SCC_CONTEXT_LIST, querySCCContexts);

                stack.addFirst(q.getVertexId());    // add itself in the stack

                for(String neighbourId:neighbourIds){
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_SCC_TYPE,
                                    new ForwardQuerySCC(context.self().id(), neighbourId, stack, q)
                            )
                            .build());
                }
            }
        }

        if(message.is(Types.QUERY_SCC_RESULT_TYPE)) {
            System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCResult received\n", context.self().id());
            QuerySCCResult result = message.as(Types.QUERY_SCC_RESULT_TYPE);

            ArrayList<QuerySCCContext> querySCCContexts = context.storage().get(QUERY_SCC_CONTEXT_LIST).orElse(new ArrayList<>());
            QuerySCCContext querySCCContext = findSCCContext(result.getQueryId(), result.getUserId(), querySCCContexts);
            if(querySCCContext == null){
                throw new IllegalStateException("querySCCContext should not be null because the result is sent back from children nodes");
            }

            boolean sccFlag = querySCCContext.isSccFlag() || result.isSccFlag();

            int updatedLowLinkId = Integer.parseInt(querySCCContext.getAggregatedLowLinkId());

            // if is scc, then update low link id
            if(result.isSccFlag()){
                // update the low link id and store into context
                updatedLowLinkId = Math.min(Integer.parseInt(result.getLowLinkId()),
                        Integer.parseInt(querySCCContext.getAggregatedLowLinkId()));
                querySCCContext.setAggregatedLowLinkId(String.valueOf(updatedLowLinkId));
                querySCCContext.setSccFlag(true);
            }


            int n = querySCCContext.getResponseNumToCollect();
            if(n-1 == 0){
                System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCResult received " +
                        "and all responses are collected\n", context.self().id());

                // if all responses are collected
                // if it is the source node, just egress
                if(context.self().id().equals(result.getQueryId())){
                    System.out.println("[StronglyConnectedComponentsFn %s] is source node, success!");
                    if(sccFlag){
                        System.out.printf("current Strongly connected component id: %s \n", updatedLowLinkId);
                    } else {
                        System.out.printf("current Strongly connected component id: %s \n", context.self().id());
                    }

                } else {    // not source code
                        // send to its parents the aggregated low link id
                    System.out.printf("[StronglyConnectedComponentsFn %s] not the source node\n", context.self().id());
                        for(String src: querySCCContext.getSourceIds()){
                            context.send(MessageBuilder
                                    .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), src)
                                    .withCustomType(
                                            Types.QUERY_SCC_RESULT_TYPE,
                                            new QuerySCCResult(result.getQueryId(), result.getUserId(),
                                                    result.getVertexId(), result.getQueryType(), String.valueOf(updatedLowLinkId), sccFlag)
                                    )
                                    .build());
                        }
                }
                // no longer need the SCC context of current node
                querySCCContexts.remove(querySCCContext);

            }else{  // not the last result to collect
                System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCResult received " +
                        "but not all responses are collected\n", context.self().id());
                querySCCContext.setResponseNumToCollect(n - 1);
                // store the current smallest low link id
                context.storage().set(QUERY_SCC_CONTEXT_LIST, querySCCContexts);
            }
        }

        return context.done();
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

    private QuerySCCContext findSCCContext(String queryId, String userId, ArrayList<QuerySCCContext> list) {
        for(QuerySCCContext e: list) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }
}
