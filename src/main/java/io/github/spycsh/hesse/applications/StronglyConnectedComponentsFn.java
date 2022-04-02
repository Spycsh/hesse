package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * this function serves the temporal query of the graph about the strongly connected component
 * e.g. it can answer at time T the vertex's connected component
 */
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
            /*
              the first node only need to receive neighbourIds.size() results
              set the component id to itself
             */
            ArrayDeque<String> firstStk = new ArrayDeque<String>() {{
                add(context.self().id());
            }};
            SCCPathContext sccPathContext = new SCCPathContext(generateNewStackHash(firstStk), context.self().id(), false, neighbourIds.size(), new HashSet<>());
            querySCCContexts.add(new QuerySCCContext(q.getQueryId(), q.getUserId(),
                    new ArrayList<SCCPathContext>(){{add(sccPathContext);}}));

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

            context.storage().set(QUERY_SCC_CONTEXT_LIST, querySCCContexts);
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
            // do backtracking
            if(q.getVertexId().equals(context.self().id())){
                System.out.printf("[StronglyConnectedComponentsFn %s] ForwardQuerySCCWithState received and there is" +
                        " a SCC back to source node\n", context.self().id());
                // if there exists a path back to the original source
                // start to aggregate low link values returned back to the original source
                if(querySCCContext == null){
                    throw new IllegalStateException("querySCCContext " + context.self().id() + " should not be null because it is on the stack");
                } else {
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), q.getSource())
                            .withCustomType(
                                    Types.QUERY_SCC_RESULT_TYPE,
                                    new QuerySCCResult(q.getQueryId(), q.getUserId(),
                                            q.getVertexId(), q.getQueryType(), context.self().id(), true, new HashSet<>(), stack)
                            )
                            .build());
                }

            }else if(stack.contains(context.self().id()) && checkConsecutiveCycle(new ArrayDeque<>(stack))){
                /*
                  if the stack contains self, it can be a loop not belonging to the scc
                  see query_scc_3.txt
                  e.g. 2->3->0->4->3->0->4 here has no scc
                  but it can also be another way pointed back to the source id
                  e.g. 2->3->0->4->3->1->2 here is a scc
                  one way is to record that if 3->0->4 occurs twice consecutively, stop forwarding
                 */

                // this path does not contains a scc
                // set sccFlag false
                // see query_scc_3.txt, node 2
                // namely send QuerySCCResult with the scc flag false
                System.out.printf("[StronglyConnectedComponentsFn %s] ForwardQuerySCCWithState received and " +
                        "fail to find a SCC\n", context.self().id());

                // just send the result back to its source with the sccFlag false
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), q.getSource())
                        .withCustomType(
                                Types.QUERY_SCC_RESULT_TYPE,
                                new QuerySCCResult(q.getQueryId(), q.getUserId(),
                                        q.getVertexId(), q.getQueryType(), context.self().id(), false, new HashSet<>(), stack)
                        )
                        .build());

            } else {
                // if it has not been on the stack, or has no two consecutive duplicated cycles
                // pushes self into the stack and continues to forward to neighbours
                System.out.printf("[StronglyConnectedComponentsFn %s] ForwardQuerySCCWithState received " +
                                "and the node has neighbours and is not on the stack\n", context.self().id());

                stack.addFirst(context.self().id());    // add itself in the stack


                 //the children nodes need to receive neighbourIds.size() results
                 // for all the parent paths pointing to it
                int newStackHash = generateNewStackHash(stack);

                // if querySCCContext is null, it must have not been visited
                if(querySCCContext == null){
                    SCCPathContext sccPathContext = new SCCPathContext(newStackHash, context.self().id(), false, neighbourIds.size(), new HashSet<>());
                    querySCCContext = new QuerySCCContext(q.getQueryId(), q.getUserId(), new ArrayList<SCCPathContext>(){{add(sccPathContext);}});
                    querySCCContexts.add(querySCCContext);
                } else{
                    // there is already a path to the current node so there is a querySCCContext
                    // so just add the current path context in current node
                    ArrayList<SCCPathContext> sccPathContexts = querySCCContext.getSccPathContexts();
                    sccPathContexts.add(new SCCPathContext(newStackHash, context.self().id(), false, neighbourIds.size(), new HashSet<>()));
                }

                context.storage().set(QUERY_SCC_CONTEXT_LIST, querySCCContexts);

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
                throw new IllegalStateException("querySCCContext should not be null because the result is sent back from children nodes\n");
            }

            ArrayDeque<String> stack = result.getStack();
            int stackHash = generateNewStackHash(stack);

            // find in context the current response num to collect by stackHash
            ArrayList<SCCPathContext> sccPathContexts = querySCCContext.getSccPathContexts();
            SCCPathContext sccContextByPathHash = findSCCContextByPathHash(sccPathContexts, stackHash);

            if(sccContextByPathHash == null){
                throw new IllegalStateException("sccContextByPathHash should not be null because the result is sent back from children nodes\n");
            }
            int n = sccContextByPathHash.getResponseNum();

            // remove the first of the stack namely itself
            stack.removeFirst();

            boolean sccFlag = sccContextByPathHash.isSccFlag() || result.isSccFlag();

            int updatedLowLinkId = Integer.parseInt(sccContextByPathHash.getAggregatedLowLinkId());

            // if is scc, then update low link id
            if(result.isSccFlag()){
                // update the low link id and store into context
                updatedLowLinkId = Math.min(Integer.parseInt(result.getLowLinkId()), updatedLowLinkId);
                sccContextByPathHash.setAggregatedLowLinkId(String.valueOf(updatedLowLinkId));
                sccContextByPathHash.getAggregatedSCCIds().addAll(result.getAggregatedSCCIds());

                sccContextByPathHash.setSccFlag(true);
            }

            // all responses are collected for one path
            if(n - 1 == 0){
                System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCResult received " +
                        "and all responses are collected for one path\n", context.self().id());

                // if it is the source node, just egress
                if(context.self().id().equals(result.getVertexId())){
                    System.out.printf("[StronglyConnectedComponentsFn %s] is source node, success!\n", context.self().id());
                    System.out.printf("[StronglyConnectedComponentsFn %s] Result of query %s by user %s: Strongly connected component id of node %s is %s \n",
                            context.self().id(), result.getQueryId(), result.getUserId(), context.self().id(), updatedLowLinkId);

                    System.out.printf("[ConnectedComponentsFn %s] Other node ids that contain in the same component are: ",
                            context.self().id());
                    // merge all ids
                    List<String> aggregatedSCCIds = new ArrayList<>();
                    for(SCCPathContext c: sccPathContexts){
                        aggregatedSCCIds.addAll(c.getAggregatedSCCIds());
                    }
                    for(String id : aggregatedSCCIds)
                        System.out.print(id + " ");
                    System.out.println();
                } else {
                    // not source node, send to its parents the aggregated low link id
                    System.out.printf("[StronglyConnectedComponentsFn %s] not the source node\n", context.self().id());

                    // merge all the cc ids, send to parent
                    Set<String> aggregatedSCCIds = new HashSet<>();
                    for(SCCPathContext c: sccPathContexts){
                        aggregatedSCCIds.addAll(c.getAggregatedSCCIds());
                    }
                    aggregatedSCCIds.add(context.self().id());

                    // backtracking
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), stack.getFirst())
                            .withCustomType(
                                    Types.QUERY_SCC_RESULT_TYPE,
                                    new QuerySCCResult(result.getQueryId(), result.getUserId(),
                                            result.getVertexId(), result.getQueryType(), String.valueOf(updatedLowLinkId), sccFlag, aggregatedSCCIds, stack)
                            )
                            .build());
                }

                // no longer need the SCC context of current node
                // querySCCContexts.remove(querySCCContext);
                // remove the pathContext using stackHash
                querySCCContext.getSccPathContexts().remove(sccContextByPathHash);
                if(querySCCContext.getSccPathContexts().size() == 0){
                    // if no paths
                    // no longer need the SCC context of current node
                    querySCCContexts.remove(querySCCContext);
                }
            } else {  // not the last result to collect
                System.out.printf("[StronglyConnectedComponentsFn %s] QuerySCCResult received " +
                        "but not all responses of one path are collected\n", context.self().id());
                sccContextByPathHash.setResponseNum(n - 1);
            }
            context.storage().set(QUERY_SCC_CONTEXT_LIST, querySCCContexts);
        }

        return context.done();
    }

    private boolean checkConsecutiveCycle(ArrayDeque<String> stack) {
        Queue<String> q = new LinkedList<>();
        while(stack.peek() != null && !q.contains(stack.peek())){
            q.offer(stack.poll());
        }
        while(q.size() != 0){
            String e1 = q.poll();
            String e2 = stack.poll();
            if(e2 == null || !e2.equals(e1)){
                return false;
            }
        }
        return true;
    }

    private SCCPathContext findSCCContextByPathHash(ArrayList<SCCPathContext> sccPathContexts, int stackHash) {
        for(SCCPathContext c:sccPathContexts){
            if(c.getPathHash() == stackHash){
                return c;
            }
        }
        return null;
    }

    private int generateNewStackHash(ArrayDeque<String> stack) {
        StringBuilder sb = new StringBuilder();
        for(String s:stack){
            sb.append(s).append(" ");
        }
        return sb.toString().hashCode();
    }

    private ArrayList<String> recoverStateAtT(int T, List<VertexActivity> activityLog){
        return recoverStateByTimeRegion(0, T, activityLog);
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

    private QuerySCCContext findSCCContext(String queryId, String userId, ArrayList<QuerySCCContext> list) {
        for(QuerySCCContext e: list) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }
}