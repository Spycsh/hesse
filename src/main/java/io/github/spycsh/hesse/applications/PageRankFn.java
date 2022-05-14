package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.pagerank.*;
import io.github.spycsh.hesse.util.Utils;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

// https://en.wikipedia.org/wiki/PageRank
public class PageRankFn implements StatefulFunction {

    public static final double DAMPING_FACTOR = 0.5;
    private static final ValueSpec<ArrayList<PageRankContext>> PAGE_RANK_CONTEXT_LIST
            = ValueSpec.named("pagerank_context_list").withCustomType(Types.PAGERANK_CONTEXT_LIST_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "pagerank");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(PageRankFn::new)
            .withValueSpecs(PAGE_RANK_CONTEXT_LIST)
            .build();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PageRankFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        // in this first round, each PageRankFn firstly receive the pagerank task with state
        // it needs to recover its in-degree and neighbours by using the state and store in context for next computation
        if(message.is(Types.PAGERANK_TASK_WITH_STATE_TYPE)) {
            LOGGER.debug("[PageRankFn {}] PageRankTaskWithState received", context.self().id());

            PageRankTaskWithState q = message.as(Types.PAGERANK_TASK_WITH_STATE_TYPE);
            Map<String, String> neighbourIdsWithWeight = Utils.recoverWeightedStateByLog(q.getVertexActivities());
            int inDegree = Utils.recoverInDegreeByLog(q.getVertexActivities());

            // if in this time window this node does not exist
            // set its pagerankValue null
            if(neighbourIdsWithWeight.size() == 0 && inDegree == 0){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), q.getCoordinatorId())
                        .withCustomType(Types.QUERY_PAGERANK_RESULT_TYPE,
                                new QueryPageRankResult(q.getQueryId(), q.getUserId(), context.self().id(), null))
                        .build());

                ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
                pageRankContexts.add(new PageRankContext(q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(), 0.0,
                        0.0, 0, inDegree, neighbourIdsWithWeight, q.getCoordinatorId()));
                context.storage().set(PAGE_RANK_CONTEXT_LIST, pageRankContexts);
            } else if(inDegree == 0) {
                // the node exists
                // only has outgoing edge, will not receive any messages
                // should directly send a message back to coordinator
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), q.getCoordinatorId())
                        .withCustomType(Types.QUERY_PAGERANK_RESULT_TYPE,
                                new QueryPageRankResult(q.getQueryId(), q.getUserId(), context.self().id(), String.valueOf(1-DAMPING_FACTOR)))
                        .build());

                ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
                pageRankContexts.add(new PageRankContext(q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(), 1-DAMPING_FACTOR,
                        0.0, 0, inDegree, neighbourIdsWithWeight, q.getCoordinatorId()));
                context.storage().set(PAGE_RANK_CONTEXT_LIST, pageRankContexts);

                // for all neighbours, send own PR value with weights
                for(Map.Entry<String, String> e : neighbourIdsWithWeight.entrySet()) {
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e.getKey())
                            .withCustomType(
                                    Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE,
                                    new PageRankValueWithWeight(q.getQueryId(), q.getUserId(), 1.0, Double.parseDouble(e.getValue()), neighbourIdsWithWeight.size()))
                            .build());
                }
            } else {
                ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
                pageRankContexts.add(new PageRankContext(q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(), 1.0,
                        0.0, 0, inDegree, neighbourIdsWithWeight, q.getCoordinatorId()));
                context.storage().set(PAGE_RANK_CONTEXT_LIST, pageRankContexts);

                // for all neighbours, send own PR value with weights
                for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()) {
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e.getKey())
                            .withCustomType(
                                    Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE,
                                    new PageRankValueWithWeight(q.getQueryId(), q.getUserId(), 1.0, Double.parseDouble(e.getValue()), neighbourIdsWithWeight.size()))
                            .build());
                }
            }
        }

        // after receiving the PR value, weight and out-degree from the source vertex
        // of all incoming edges to itself, send its own result to coordinator
        if(message.is(Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE)){
            PageRankValueWithWeight p = message.as(Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE);

            LOGGER.debug("[PageRankFn {}] PageRankValueWithWeight received {},{},{}", context.self().id(), p.getPagerankValue(), p.getWeight(), p.getDegree());
            ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
            PageRankContext pageRankContext = findPageRankContext(p.getQueryId(), p.getUserId(), pageRankContexts);
            double sum = pageRankContext.getCurrentPrValue();
            sum = sum + p.getPagerankValue() * p.getWeight() / p.getDegree();
            pageRankContext.setCurrentPrValue(sum);

            // if receive enough pagerank value with weights from other nodes
            // enter the next round
            int collectedDegree = pageRankContext.getCurrentCollectedDegree();
            collectedDegree += 1;
            if(collectedDegree == pageRankContext.getInDegree()){
                pageRankContext.setCurrentCollectedDegree(0);
                double newValue = 1 - DAMPING_FACTOR + DAMPING_FACTOR * sum;

                pageRankContext.setPreviousPrValue(newValue);
                // clear the current pr value
                pageRankContext.setCurrentPrValue(0.0);

                // send current pagerank value to coordinator
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), pageRankContext.getCoordinatorId())
                        .withCustomType(Types.QUERY_PAGERANK_RESULT_TYPE,
                                new QueryPageRankResult(p.getQueryId(), p.getUserId(), context.self().id(), String.valueOf(newValue)))
                        .build());
            } else {
                pageRankContext.setPreviousPrValue(sum);
                pageRankContext.setCurrentCollectedDegree(collectedDegree);
            }

            context.storage().set(PAGE_RANK_CONTEXT_LIST, pageRankContexts);

        }

        // in this following rounds, each PageRank continue the computation with its in-degree and neighbours
        if(message.is(Types.PAGERANK_CONTINUE_TASK_TYPE)){
            LOGGER.debug("[PageRankFn {}] PageRankContinueTask received", context.self().id());

            PageRankContinueTask q = message.as(Types.PAGERANK_CONTINUE_TASK_TYPE);
            ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
            PageRankContext pageRankContext = findPageRankContext(q.getQueryId(), q.getUserId(), pageRankContexts);

            if(pageRankContext.getInDegree() == 0 && pageRankContext.getNeighbourIdsWithWeight().size() == 0){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), pageRankContext.getCoordinatorId())
                        .withCustomType(Types.QUERY_PAGERANK_RESULT_TYPE,
                                new QueryPageRankResult(q.getQueryId(), q.getUserId(), context.self().id(), null))
                        .build());
            }else if(pageRankContext.getInDegree() == 0) {
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), pageRankContext.getCoordinatorId())
                        .withCustomType(Types.QUERY_PAGERANK_RESULT_TYPE,
                                new QueryPageRankResult(q.getQueryId(), q.getUserId(), context.self().id(), String.valueOf(1-DAMPING_FACTOR)))
                        .build());
                Map<String, String> neighbourIdsWithWeight = pageRankContext.getNeighbourIdsWithWeight();

                // for all neighbours, send own PR value with weights
                for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()) {
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e.getKey())
                            .withCustomType(
                                    Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE,
                                    new PageRankValueWithWeight(q.getQueryId(), q.getUserId(),
                                            pageRankContext.getPreviousPrValue(), Double.parseDouble(e.getValue()),
                                            neighbourIdsWithWeight.size()))
                            .build());
                }
            } else{
                Map<String, String> neighbourIdsWithWeight = pageRankContext.getNeighbourIdsWithWeight();

                // for all neighbours, send own PR value with weights
                for(Map.Entry<String, String> e: neighbourIdsWithWeight.entrySet()) {
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e.getKey())
                            .withCustomType(
                                    Types.PAGERANK_VALUE_WITH_WEIGHT_TYPE,
                                    new PageRankValueWithWeight(q.getQueryId(), q.getUserId(),
                                            pageRankContext.getPreviousPrValue(), Double.parseDouble(e.getValue()),
                                            neighbourIdsWithWeight.size()))
                            .build());
                }
            }
        }

        if(message.is(Types.PAGERANK_CONTEXT_CLEAR_TYPE)){
            LOGGER.debug("[PageRankFn {}] PageRankContextClear received", context.self().id());

            PageRankContextClear p = message.as(Types.PAGERANK_CONTEXT_CLEAR_TYPE);
            ArrayList<PageRankContext> pageRankContexts = context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>());
            PageRankContext pageRankContext = findPageRankContext(p.getQueryId(), p.getUserId(), context.storage().get(PAGE_RANK_CONTEXT_LIST).orElse(new ArrayList<>()));
            pageRankContexts.remove(pageRankContext);
        }

        return context.done();
    }

    private PageRankContext findPageRankContext(String queryId, String userId, ArrayList<PageRankContext> list) {
        for(PageRankContext e: list) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }
}
