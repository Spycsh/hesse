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

            if(neighbourIds.size() == 0){
                StringBuilder result = new StringBuilder();
                String str1 = String.format("Result of query %s by user %s: no connected component found for node %s because it is isolated!",
                        q.getQueryId(), q.getUserId(), context.self().id());
                result.append(str1);
                sendResult(context, q.getQueryId(), q.getUserId(), result.toString());
            }else{
                HashSet<String> vertexSet = new HashSet<String>(){{
                    add(context.self().id());
                }};
                vertexSet.addAll(neighbourIds);

                QueryCCContext c = new QueryCCContext(q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(),
                        vertexSet, new HashSet<>(), neighbourIds.size());
                ArrayList<QueryCCContext> list = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());
                list.add(c);
                context.storage().set(QUERY_CC_CONTEXT_LIST, list);

                for(String neighbourId: neighbourIds){
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                            .withCustomType(
                                    Types.FORWARD_QUERY_CC_TYPE,
                                    new ForwardQueryCC(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getStartT(), q.getEndT())
                            )
                            .build());
                }
            }
        }

        if(message.is(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE)) {
            LOGGER.debug("[ConnectedComponentsFn {}] ForwardQueryCCWithState received", context.self().id());
            ForwardQueryCCWithState q = message.as(Types.FORWARD_QUERY_CC_WITH_STATE_TYPE);
            HashSet<String> neighbourIds = Utils.recoverStateByLog(q.getVertexActivities());

            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", "connected-components"), q.getVertexId())
                    .withCustomType(
                            Types.QUERY_CC_NEIGHBOURS_TYPE,
                            new QueryCCNeighbours(q.getQueryId(), q.getUserId(), q.getVertexId(), neighbourIds)
                    )
                    .build());
        }

        if(message.is(Types.QUERY_CC_NEIGHBOURS_TYPE)) {
            LOGGER.debug("[ConnectedComponentsFn {}] QueryCCNeighbours received", context.self().id());
            QueryCCNeighbours q = message.as(Types.QUERY_CC_NEIGHBOURS_TYPE);
            ArrayList<QueryCCContext> list = context.storage().get(QUERY_CC_CONTEXT_LIST).orElse(new ArrayList<>());

            QueryCCContext ccContext = findCCContext(q.getQueryId(), q.getUserId(), list);
            assert ccContext != null;
            HashSet<String> vertexSet = ccContext.getVertexSet();

            HashSet<String> currentCandidateNeighbours = ccContext.getCandidateNeighbours();
            HashSet<String> candidateNeighbours = q.getNeighbours();
            candidateNeighbours.removeAll(vertexSet);
            currentCandidateNeighbours.addAll(candidateNeighbours);
            ccContext.setCandidateNeighbours(currentCandidateNeighbours);

            vertexSet.addAll(q.getNeighbours());

            ccContext.setCandidateResponseNumber(ccContext.getCandidateResponseNumber() - 1);

            if(ccContext.getCandidateResponseNumber() == 0) {
                if(currentCandidateNeighbours.size() == 0){
                    /**
                     * egress!
                     */
                    int lowLinkId = Integer.MAX_VALUE;
                    for(String i: vertexSet){
                        lowLinkId = Math.min(Integer.parseInt(i), lowLinkId);
                    }
                    String str1 = String.format("Result of query %s by user %s: connected component id of node %s is %s. ",
                            q.getQueryId(), q.getUserId(), context.self().id(), lowLinkId);
                    String str2 = "All node ids that contain in the same component are:" + vertexSet.toString();

                    sendResult(context, q.getQueryId(), q.getUserId(), str1 + str2);

                    // clear state
                    removeCCContext(q.getQueryId(), q.getUserId(), list);
                }else{
                    // clear candidate neighbors and set candidate response number
                    // continue forwarding to current candidate neighbours
                    ccContext.setCandidateResponseNumber(currentCandidateNeighbours.size());
                    ccContext.setCandidateNeighbours(new HashSet<>());

                    for(String neighbourId: currentCandidateNeighbours){
                        context.send(MessageBuilder
                                .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), neighbourId)
                                .withCustomType(
                                        Types.FORWARD_QUERY_CC_TYPE,
                                        new ForwardQueryCC(q.getQueryId(), q.getUserId(), q.getVertexId(),
                                                ccContext.getStartT(), ccContext.getEndT())
                                )
                                .build());
                    }
                }
            }
            context.storage().set(QUERY_CC_CONTEXT_LIST, list);
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

    private void removeCCContext(String queryId, String userId, ArrayList<QueryCCContext> list) {
        list.removeIf(e -> e.getQueryId().equals(queryId) && e.getUserId().equals(userId));
    }
}
