package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.cc.*;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.util.Utils;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import org.slf4j.LoggerFactory;

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

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ConnectedComponentsFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        if(message.is(Types.QUERY_CC_WITH_STATE_TYPE)){
            LOGGER.debug("[ConnectedComponentsFn {}] QueryCCWithState received", context.self().id());
            QueryCCWithState q = message.as(Types.QUERY_CC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();

            HashSet<String> neighbourIds = Utils.recoverStateByLog(vertexActivities);

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            ArrayDeque<String> firstStk = new ArrayDeque<String>() {{
                add(context.self().id());
            }};
            CCPathContext ccPathContext = new CCPathContext(Utils.generateNewStackHash(firstStk), context.self().id(), neighbourIds.size(), new HashSet<>());
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
            context.storage().set(QUERY_CC_CONTEXT_LIST, queryCCContexts);
        }

        if(message.is(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE)){
            LOGGER.debug("[ConnectedComponentsFn {}] ForwardQueryCCWithState received", context.self().id());

            ForwardQueryCCWithState q = message.as(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();
            HashSet<String> neighbourIds = Utils.recoverStateByLog(vertexActivities);

            ArrayDeque<String> stack = q.getStack();

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryCCContext queryCCContext = findCCContext(q.getQueryId(), q.getUserId(), queryCCContexts);

            // if self already on stack path or has no more neighbours other than the node that the message comes from,
            // then return response
            // otherwise, continue forwarding
            if(stack.contains(context.self().id()) || neighbourIds.size() == 0){
                LOGGER.debug("[ConnectedComponentsFn {}] ForwardQueryCCWithState received and self " +
                        "is already on the stack or has no more neighbours", context.self().id());

                Set<String> aggregatedCCIds = new HashSet<>();

                if(neighbourIds.size() == 0){
                    aggregatedCCIds.add(context.self().id());
                }
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
                LOGGER.debug("[ConnectedComponentsFn {}] ForwardQueryCCWithState received and self " +
                        "is not on the stack, continue forwarding", context.self().id());

                stack.addFirst(context.self().id());    // add itself in the stack
                int newStackHash = Utils.generateNewStackHash(stack);

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
            LOGGER.debug("[ConnectedComponentsFn {}] QueryCCResult received", context.self().id());
            QueryCCResult result = message.as(Types.QUERY_CC_RESULT_TYPE);

            ArrayList<QueryCCContext> queryCCContexts = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
            QueryCCContext queryCCContext = findCCContext(result.getQueryId(), result.getUserId(), queryCCContexts);
            if(queryCCContext == null){
                LOGGER.error("[ConnectedComponentsFn {}] queryCCContext should not be null because the result is sent back from children nodes", context.self().id());
                throw new IllegalStateException("queryCCContext should not be null because the result is sent back from children nodes\n");
            }

            ArrayDeque<String> stack = result.getStack();
            int stackHash = Utils.generateNewStackHash(stack);

            // find in context the current response num to collect by stackHash
            ArrayList<CCPathContext> ccPathContexts = queryCCContext.getCcPathContexts();
            CCPathContext ccContextByPathHash = findCCContextByPathHash(ccPathContexts, stackHash);

            // add all the aggregated cc ids for one path from descendants
            if(ccContextByPathHash == null){
                LOGGER.error("[ConnectedComponentsFn {}] ccContextByPathHash should not be null!", context.self().id());
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
                LOGGER.debug("[ConnectedComponentsFn {}] QueryCCResult received " +
                                "and all responses are collected for one path", context.self().id());

                // if it is the source node, just egress
                if(context.self().id().equals(result.getVertexId())){
                    LOGGER.info("[ConnectedComponentsFn {}] Success! qid: {}, uid: {}",
                            context.self().id(), result.getQueryId(), result.getUserId());
                    String str1 = String.format("Result of query %s by user %s: connected component id of node %s is %s. ",
                            result.getQueryId(), result.getUserId(), context.self().id(), updatedLowLinkId);
                    String str2 = "Other node ids that contain in the same component are:";

                    StringBuilder sb = new StringBuilder();
                    for(String id : ccContextByPathHash.getAggregatedCCIds())
                        sb.append(" ").append(id);

                    String resultStr = str1 + str2 + sb.toString();

                    sendResult(context, result.getQueryId(), result.getUserId(), resultStr);

                } else {
                    // not source node, send to its parents the aggregated low link id
                    LOGGER.debug("[ConnectedComponentsFn {}}] not the source node", context.self().id());
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
                LOGGER.debug("[ConnectedComponentsFn {}] QueryCCResult received " +
                                "but not all responses of one path are collected", context.self().id());
                ccContextByPathHash.setResponseNum(n - 1);
            }
            context.storage().set(QUERY_CC_CONTEXT_LIST, queryCCContexts);
        }

        return context.done();
    }

    private void sendResult(Context context, String queryId, String userId, String resultStr) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.query", "temporal-query-handler"), queryId)
                .withCustomType(
                        Types.QUERY_RESULT_TYPE,
                        new QueryResult(queryId, userId, resultStr))
                .build());
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
