package io.github.spycsh.hesse.coordination;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.pagerank.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

/**
 * different query will be forwarded to different CoordinatorFn to promise concurrency of handling
 * different queries In contrast, application instance use queryId and userId to promise concurrency
 */
public class CoordinatorFn implements StatefulFunction {

  // all the ids in the graph from the beginning of time
  private static final ValueSpec<Set<String>> GRAPH_IDS =
      ValueSpec.named("graphIDs").withCustomType(Types.GRAPH_IDS_TYPE);
  // iterations
  private static final ValueSpec<Integer> ITERATIONS = ValueSpec.named("iterations").withIntType();
  // current iteration
  private static final ValueSpec<Integer> CURRENT_ITERATION =
      ValueSpec.named("currentIteration").withIntType();
  // number of responses to collect from PageRank applications
  private static final ValueSpec<Integer> RESPONSE_TO_COLLECT =
      ValueSpec.named("responseToCollect").withIntType();
  // number of prepare responses to collect from PageRank applications
  private static final ValueSpec<Integer> PREPARE_RESPONSE_TO_COLLECT =
      ValueSpec.named("prepareResponseToCollect").withIntType();
  // current collected response number
  private static final ValueSpec<Integer> CURRENT_RESPONSE_COLLECTED =
      ValueSpec.named("currentResponseCollected").withIntType();
  // current collected prepare phase response number
  private static final ValueSpec<Integer> CURRENT_PREPARE_RESPONSE_COLLECTED =
      ValueSpec.named("currentPrepareResponseCollected").withIntType();

  // page rank results for each round
  private static final ValueSpec<Map<Integer, Map<String, String>>> PAGERANK_RESULTS =
      ValueSpec.named("pagerankResults").withCustomType(Types.PAGERANK_RESULTS_TYPE);

  private static final TypeName TYPE_NAME =
      TypeName.typeNameOf("hesse.coordination", "coordinator");

  public static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPE_NAME)
          .withSupplier(CoordinatorFn::new)
          .withValueSpecs(
              GRAPH_IDS,
              ITERATIONS,
              CURRENT_ITERATION,
              RESPONSE_TO_COLLECT,
              CURRENT_RESPONSE_COLLECTED,
              PREPARE_RESPONSE_TO_COLLECT,
              CURRENT_PREPARE_RESPONSE_COLLECTED,
              PAGERANK_RESULTS)
          .build();

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(CoordinatorFn.class);

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    // coordinator 00 receives a set of all vertex ids in the graph
    // for collecting enough responses
    if (message.isUtf8String()) {
      String srcIdToDstId = message.asUtf8String();

      Set<String> graphIds = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
      String[] srcToDst = srcIdToDstId.split(" ");
      graphIds.add(srcToDst[0]);
      graphIds.add(srcToDst[1]);

      LOGGER.debug(
          "[CoordinatorFn {}] graph id {},{} received",
          context.self().id(),
          srcToDst[0],
          srcToDst[1]);

      context.storage().set(GRAPH_IDS, graphIds);
    }

    // the current query coordinator receives the pagerank query from query handler
    if (message.is(Types.QUERY_PAGERANK_TYPE)) {
      LOGGER.debug("[CoordinatorFn {}] QueryPageRank received", context.self().id());
      QueryPageRank q = message.as(Types.QUERY_PAGERANK_TYPE);
      if (q.getVertexId().equals("all")) {
        int iterations = q.getIterations();
        context.storage().set(ITERATIONS, iterations);

        Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());

        if (set.size() == 0) {
          // one coordinator named after queryId_userId is responsible for one query
          // all coordinators ask for coordinator 0 for graph ids
          context.send(
              MessageBuilder.forAddress(
                      TypeName.typeNameOf("hesse.coordination", "coordinator"), "0")
                  .withCustomType(
                      Types.QUERY_GRAPH_IDS_TYPE,
                      new QueryGraphIds(
                          q.getQueryId(),
                          q.getUserId(),
                          q.getStartT(),
                          q.getEndT(),
                          context.self().id()))
                  .build());
        }

      } else {
        LOGGER.error(
            "[CoordinatorFn {}] Currently only support PageRank on whole graph",
            context.self().id());
      }
    }

    // coordinator 0 receives requests of all graph ids
    if (message.is(Types.QUERY_GRAPH_IDS_TYPE)) {
      LOGGER.debug("[CoordinatorFn {}] QueryGraphIds received", context.self().id());
      QueryGraphIds q = message.as(Types.QUERY_GRAPH_IDS_TYPE);

      Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());

      context.send(
          MessageBuilder.forAddress(
                  TypeName.typeNameOf("hesse.coordination", "coordinator"), q.getSourceId())
              .withCustomType(
                  Types.RESPONSE_GRAPH_IDS_TYPE,
                  new ResponseGraphIds(
                      q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(), set))
              .build());
    }

    // the current query coordinator receives all the graph ids
    // and broad cast tasks to all the graph vertices
    if (message.is(Types.RESPONSE_GRAPH_IDS_TYPE)) {
      LOGGER.debug("[CoordinatorFn {}] ResponseGraphIds received", context.self().id());
      ResponseGraphIds q = message.as(Types.RESPONSE_GRAPH_IDS_TYPE);
      Set<String> set = q.getGraphIds();
      context.storage().set(GRAPH_IDS, set);

      context.storage().set(RESPONSE_TO_COLLECT, set.size());
      context.storage().set(CURRENT_RESPONSE_COLLECTED, 0);
      context.storage().set(PREPARE_RESPONSE_TO_COLLECT, set.size());
      context.storage().set(CURRENT_PREPARE_RESPONSE_COLLECTED, 0);
      broadcastPrepareTask(context, set, q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT());
    }

    if (message.is(Types.PAGERANK_PREPARE_RESPONSE_TYPE)) {
      PageRankPrepareResponse q = message.as(Types.PAGERANK_PREPARE_RESPONSE_TYPE);
      LOGGER.debug("[CoordinatorFn {}] PageRankPrepareResponse received", context.self().id());

      int currentPrepareResponseCollected =
          context.storage().get(CURRENT_PREPARE_RESPONSE_COLLECTED).orElse(0);
      currentPrepareResponseCollected += 1;
      context.storage().set(CURRENT_PREPARE_RESPONSE_COLLECTED, currentPrepareResponseCollected);
      int prepareResponseToCollect = context.storage().get(PREPARE_RESPONSE_TO_COLLECT).orElse(0);
      LOGGER.debug(
          "[CoordinatorFn {}] prepare response to collect: {}",
          context.self().id(),
          prepareResponseToCollect);
      LOGGER.debug(
          "[CoordinatorFn {}] current collect: {}",
          context.self().id(),
          currentPrepareResponseCollected);

      if (currentPrepareResponseCollected == prepareResponseToCollect) {
        Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
        broadcastContinueTask(context, set, q.getQueryId(), q.getUserId());
      }
    }

    // receive the pagerank value from one vertex
    if (message.is(Types.QUERY_PAGERANK_RESULT_TYPE)) {
      QueryPageRankResult q = message.as(Types.QUERY_PAGERANK_RESULT_TYPE);
      LOGGER.debug(
          "[CoordinatorFn {}] QueryPageRankResult received from {}",
          context.self().id(),
          q.getVertexId());

      int currentResponseCollected = context.storage().get(CURRENT_RESPONSE_COLLECTED).orElse(0);
      currentResponseCollected += 1;
      int responseToCollect = context.storage().get(RESPONSE_TO_COLLECT).orElse(0);
      LOGGER.debug(
          "[CoordinatorFn {}] response to collect: {}", context.self().id(), responseToCollect);
      LOGGER.debug(
          "[CoordinatorFn {}] current collect: {}", context.self().id(), currentResponseCollected);

      int currentIteration = context.storage().get(CURRENT_ITERATION).orElse(1);
      int iterations = context.storage().get(ITERATIONS).orElse(0);

      Map<Integer, Map<String, String>> res =
          context.storage().get(PAGERANK_RESULTS).orElse(new HashMap<>());

      // store the new pagerank value into the overall result map
      Map<String, String> oneIterationResult = res.getOrDefault(currentIteration, new HashMap<>());
      oneIterationResult.put(q.getVertexId(), q.getPagerankValue());
      res.put(currentIteration, oneIterationResult);
      context.storage().set(PAGERANK_RESULTS, res);

      if (currentResponseCollected == responseToCollect) {
        LOGGER.debug(
            "[CoordinatorFn {}] Current iteration: {}", context.self().id(), currentIteration);
        // clear for next round
        context.storage().set(CURRENT_RESPONSE_COLLECTED, 0);

        if (currentIteration == iterations) {
          // egress
          System.out.println("success!");

          // egress the pagerank result
          context.send(
              MessageBuilder.forAddress(
                      TypeName.typeNameOf("hesse.query", "temporal-query-handler"), q.getQueryId())
                  .withCustomType(
                      Types.QUERY_RESULT_TYPE,
                      new QueryResult(q.getQueryId(), q.getUserId(), res.toString()))
                  .build());

          // clear all state of this query
          clearPageRankWorkerState(
              context,
              context.storage().get(GRAPH_IDS).orElse(new HashSet<>()),
              q.getQueryId(),
              q.getUserId());
          clearCoordinatorState(context);
        } else {
          context.storage().set(CURRENT_ITERATION, currentIteration);
          Set<String> set = context.storage().get(GRAPH_IDS).orElse(new HashSet<>());
          broadcastContinueTask(context, set, q.getQueryId(), q.getUserId());
        }

        currentIteration += 1;
        context.storage().set(CURRENT_ITERATION, currentIteration);
      } else {
        context.storage().set(CURRENT_RESPONSE_COLLECTED, currentResponseCollected);
      }
    }

    return context.done();
  }

  // broadcast pagerank tasks to PageRankFn instances
  public void broadcastPrepareTask(
      Context context, Set<String> set, String qid, String uid, int startT, int endT) {
    for (String e : set) {
      context.send(
          MessageBuilder.forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), e)
              .withCustomType(
                  Types.PAGERANK_PREPARE_TASK_TYPE,
                  new PageRankPrepareTask(qid, uid, startT, endT, context.self().id()))
              .build());
    }
  }

  public void broadcastContinueTask(Context context, Set<String> set, String qid, String uid) {
    for (String e : set) {
      context.send(
          MessageBuilder.forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e)
              .withCustomType(Types.PAGERANK_CONTINUE_TASK_TYPE, new PageRankContinueTask(qid, uid))
              .build());
    }
  }

  private void clearCoordinatorState(Context context) {
    context.storage().remove(GRAPH_IDS);
    context.storage().remove(ITERATIONS);
    context.storage().remove(CURRENT_ITERATION);
    context.storage().remove(RESPONSE_TO_COLLECT);
    context.storage().remove(CURRENT_RESPONSE_COLLECTED);
    context.storage().remove(PAGERANK_RESULTS);
    context.storage().remove(PREPARE_RESPONSE_TO_COLLECT);
    context.storage().remove(CURRENT_PREPARE_RESPONSE_COLLECTED);
  }

  private void clearPageRankWorkerState(Context context, Set<String> set, String qid, String uid) {
    for (String e : set) {
      context.send(
          MessageBuilder.forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), e)
              .withCustomType(Types.PAGERANK_CONTEXT_CLEAR_TYPE, new PageRankContextClear(qid, uid))
              .build());
    }
  }
}
