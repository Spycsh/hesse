package io.github.spycsh.hesse.coordination;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.pagerank.PageRankContinueTask;
import io.github.spycsh.hesse.types.pagerank.PageRankTask;
import io.github.spycsh.hesse.types.pagerank.QueryPageRank;
import io.github.spycsh.hesse.types.pagerank.QueryPageRankResult;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * different query will be forwarded to different CoordinatorFn to promise concurrency of handling different queries
 * In contrast, application instance use queryId and userId to promise concurrency
 */
public class CoordinatorFn implements StatefulFunction {

    // all the ids in the graph from the beginning of time
    private static final ValueSpec<Set<String>> GRAPH_IDS = ValueSpec.named("graphIDs").withCustomType(Types.GRAPH_IDS_TYPE);
    // iterations
    private static final ValueSpec<Integer> ITERATIONS = ValueSpec.named("iterations").withIntType();
    // current iteration
    private static final ValueSpec<Integer> CURRENT_ITERATION = ValueSpec.named("currentIteration").withIntType();
    // number of responses to collect from PageRank applications
    private static final ValueSpec<Integer> RESPONSE_TO_COLLECT = ValueSpec.named("responseToCollect").withIntType();
    // current collected response number
    private static final ValueSpec<Integer> CURRENT_RESPONSE_COLLECTED = ValueSpec.named("currentResponseCollected").withIntType();
    // page rank results for each round
    private static final ValueSpec<Map<Integer, Map<String, Double>>> PAGERANK_RESULTS = ValueSpec.named("pagerankResults").withCustomType(Types.PAGERANK_RESULTS_TYPE);

    private static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.coordination", "coordinator");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(CoordinatorFn::new)
            .withValueSpecs(GRAPH_IDS, ITERATIONS, CURRENT_ITERATION, RESPONSE_TO_COLLECT, CURRENT_RESPONSE_COLLECTED, PAGERANK_RESULTS)
            .build();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CoordinatorFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        // here record a set of all nodes id in the graph
        // for collecting enough responses
        if(message.isUtf8String()){
            LOGGER.debug("[CoordinatorFn {}] graph id received", context.self().id());
            String srcIdToDstId = message.asUtf8String();
            Set<String> graphIds = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
            String[] srcToDst = srcIdToDstId.split(" ");
            graphIds.add(srcToDst[0]);
            graphIds.add(srcToDst[1]);
            context.storage().set(GRAPH_IDS, graphIds);
        }

        if(message.is(Types.QUERY_PAGERANK_TYPE)){
            LOGGER.debug("[CoordinatorFn {}] QueryPageRank received", context.self().id());
            QueryPageRank q = message.as(Types.QUERY_PAGERANK_TYPE);
            if(q.getVertexId().equals("all")){
                int iterations = q.getIterations();
                context.storage().set(ITERATIONS, iterations);
                // context.storage().set(CURRENT_ITERATION, 0);

                Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
                context.storage().set(RESPONSE_TO_COLLECT, set.size());
                context.storage().set(CURRENT_RESPONSE_COLLECTED, 0);
                broadcastTask(context, set, q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT());
            }
        }

        // receive the pagerank value from one vertex
        if(message.is(Types.QUERY_PAGERANK_RESULT_TYPE)){
            LOGGER.debug("[CoordinatorFn {}] QueryPageRankResult received", context.self().id());
            QueryPageRankResult q = message.as(Types.QUERY_PAGERANK_RESULT_TYPE);
            int currentResponseCollected = context.storage().get(CURRENT_RESPONSE_COLLECTED).orElse(0);
            currentResponseCollected += 1;
            int responseToCollect = context.storage().get(RESPONSE_TO_COLLECT).orElse(0);
            System.out.println("responseToCollect:"+responseToCollect);

            int currentIteration = context.storage().get(CURRENT_ITERATION).orElse(1);
            int iterations = context.storage().get(ITERATIONS).orElse(0);

            Map<Integer, Map<String, Double>> res = context.storage().get(PAGERANK_RESULTS).orElse(new HashMap<>());

            // store the new pagerank value into the overall result map
            Map<String, Double> oneIterationResult = res.getOrDefault(currentIteration, new HashMap<>());
            oneIterationResult.put(q.getVertexId(), q.getPagerankValue());
            res.put(currentIteration, oneIterationResult);
            context.storage().set(PAGERANK_RESULTS, res);

            if(currentResponseCollected == responseToCollect){
                LOGGER.debug("[CoordinatorFn {}] Current iteration: {}", context.self().id(), currentIteration);
                // clear for next round
                context.storage().set(CURRENT_RESPONSE_COLLECTED, 0);

                currentIteration += 1;
                context.storage().set(CURRENT_ITERATION, currentIteration);

                if(currentIteration == iterations){
                    // egress
                    System.out.println("success!");

                    // egress the pagerank result
                    context.send(MessageBuilder
                            .forAddress(TypeName.typeNameOf("hesse.query", "temporal-query-handler"), q.getQueryId())
                            .withCustomType(
                                    Types.QUERY_RESULT_TYPE,
                                    new QueryResult(q.getQueryId(), q.getUserId(), res.toString()))
                            .build());

                    // clear iteration
                    context.storage().set(CURRENT_ITERATION, 0);

                }else{
                    context.storage().set(CURRENT_ITERATION, currentIteration);
                    Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
                    broadcastTask(context, set, q.getQueryId(), q.getUserId());
                }
            }else{
                context.storage().set(CURRENT_RESPONSE_COLLECTED, currentResponseCollected);

            }
        }

        return context.done();
    }

    public void broadcastTask(Context context, Set<String> set, String qid, String uid, int startT, int endT){
        for(String e : set){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), e)
                    .withCustomType(
                            Types.PAGERANK_TASK_TYPE,
                            new PageRankTask(qid, uid, startT, endT)
                    )
                    .build());
        }
    }

    public void broadcastTask(Context context, Set<String> set, String qid, String uid){
        for(String e : set){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e)
                    .withCustomType(
                            Types.PAGERANK_CONTINUE_TASK_TYPE,
                            new PageRankContinueTask(qid, uid)
                    )
                    .build());
        }
    }
}
