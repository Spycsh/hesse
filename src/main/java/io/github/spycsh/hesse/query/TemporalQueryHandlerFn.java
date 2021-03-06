package io.github.spycsh.hesse.query;

import io.github.spycsh.hesse.types.HistoryQuery;
import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.cc.QueryCC;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.gnnsampling.QueryGNNSampling;
import io.github.spycsh.hesse.types.pagerank.QueryPageRank;
import io.github.spycsh.hesse.types.scc.QuerySCC;
import io.github.spycsh.hesse.types.sssp.QuerySSSP;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

/**
 * this function handle all the queries from user it will forward the query to the targeted vertex
 * (VertexStorageFn) the state restored from that vertex
 */
public class TemporalQueryHandlerFn implements StatefulFunction {

  private static final ValueSpec<ArrayList<HistoryQuery>> QUERY_HISTORY =
      ValueSpec.named("queryHistory").withCustomType(Types.QUERY_HISTORY);

  static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "query-results");
  static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.query", "temporal-query-handler");

  public static final StatefulFunctionSpec SPEC =
      StatefulFunctionSpec.builder(TYPE_NAME)
          .withSupplier(TemporalQueryHandlerFn::new)
          .withValueSpecs(QUERY_HISTORY)
          .build();

  private static final org.slf4j.Logger LOGGER =
      LoggerFactory.getLogger(TemporalQueryHandlerFn.class);

  @Override
  public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
    if (message.is(Types.QUERY_TYPE)) {
      Query q = message.as(Types.QUERY_TYPE);
      String vertexId = q.getVertexId();

      LOGGER.info(
          "[TemporalQueryHandler {}] Received Query {} from User {} of vertex {} with query type {}",
          context.self().id(),
          q.getQueryId(),
          q.getUserId(),
          vertexId,
          q.getQueryType());

      // check whether in cache the query exist
      long queryReceiveTime = System.currentTimeMillis();
      boolean queryExist =
          checkCache(
              context,
              q.getQueryId(),
              q.getUserId(),
              q.getVertexId(),
              q.getQueryType(),
              queryReceiveTime);

      // check query type, create the corresponding message and send to storage layer
      if (!queryExist) {
        switch (q.getQueryType()) {
          case "connected-components":
            {
              context.send(
                  MessageBuilder.forAddress(
                          TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                      .withCustomType(Types.QUERY_CC_TYPE, new QueryCC(q))
                      .build());
              break;
            }
          case "strongly-connected-components":
            {
              context.send(
                  MessageBuilder.forAddress(
                          TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                      .withCustomType(Types.QUERY_SCC_TYPE, new QuerySCC(q))
                      .build());
              break;
            }
          case "gnn-sampling":
            {
              context.send(
                  MessageBuilder.forAddress(
                          TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                      .withCustomType(Types.QUERY_GNN_SAMPLING_TYPE, new QueryGNNSampling(q))
                      .build());
              break;
            }
          case "single-source-shortest-path":
            {
              context.send(
                  MessageBuilder.forAddress(
                          TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                      .withCustomType(Types.QUERY_SSSP_TYPE, new QuerySSSP(q))
                      .build());
              break;
            }
          case "pagerank":
            {
              context.send(
                  MessageBuilder.forAddress(
                          TypeName.typeNameOf("hesse.coordination", "coordinator"),
                          q.getQueryId() + "_" + q.getUserId()) // send to coordinator "qid uid"
                      .withCustomType(Types.QUERY_PAGERANK_TYPE, new QueryPageRank(q))
                      .build());
              break;
            }
        }
      }
    }

    if (message.is(Types.QUERY_RESULT_TYPE)) {
      QueryResult res = message.as(Types.QUERY_RESULT_TYPE);

      ArrayList<HistoryQuery> historyQueries =
          context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
      for (HistoryQuery hq : historyQueries) {
        if (hq.getQueryId().equals(res.getQueryId()) && hq.getUserId().equals(res.getUserId())) {
          long duration = System.currentTimeMillis() - hq.getQueryReceiveTime();
          LOGGER.info(
              "[TemporalQueryHandler {}] qid: {} uid: {} result: {} duration: {}",
              context.self().id(),
              hq.getQueryId(),
              hq.getUserId(),
              res.getResult(),
              duration);
          // produce to Kafka
          outputResultToKafka(
              context, res.getQueryId(), res.getUserId(), res.getResult(), duration);

          hq.setResult(res.getResult());
        }
      }
      context.storage().set(QUERY_HISTORY, historyQueries);
    }

    return context.done();
  }

  /** egress the result string and the duration the query takes to Kafka topic */
  private void outputResultToKafka(
      Context context, String queryId, String userId, String result, long duration) {
    context.send(
        KafkaEgressMessage.forEgress(KAFKA_EGRESS)
            .withTopic("query-results")
            .withUtf8Key(queryId + " " + userId)
            .withUtf8Value(
                String.format("{\"result_string\":\"%s\", \"time\":\"%s\"}", result, duration))
            .build());
  }

  /**
   * When performing the query, system will check whether there is already a query in the cache 1.
   * if this query has an entry in history query list and already has a result, then print it and
   * set queryExist flag to be true 2. if this query does not have a result but has an entry, that
   * means it is in processing just perform the query as a new one with current system time as
   * queryReceiveTime 3. if this query has no entry, create an entry in the history query list
   */
  private boolean checkCache(
      Context context,
      String queryId,
      String userId,
      String vertexId,
      String queryType,
      long queryReceiveTime) {
    ArrayList<HistoryQuery> historyQueries =
        context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
    boolean queryExist = false;
    for (HistoryQuery hq : historyQueries) {
      if (hq.getQueryId().equals(queryId)
          && hq.getUserId().equals(userId)
          && hq.getResult() != null) { // hit cache
        hq.setQueryReceiveTime(queryReceiveTime);
        System.out.println(hq.getResult());
        long duration = System.currentTimeMillis() - queryReceiveTime;
        LOGGER.info(
            "[TemporalQueryHandler {}] qid: {} uid: {} result: {} duration: {}",
            context.self().id(),
            hq.getQueryId(),
            hq.getUserId(),
            hq.getResult(),
            duration);
        // produce to Kafka
        outputResultToKafka(context, hq.getQueryId(), hq.getUserId(), hq.getResult(), duration);

        queryExist = true;
      } else if (hq.getQueryId().equals(queryId)
          && hq.getUserId().equals(userId)
          && hq.getResult() == null) {
        historyQueries.add(
            new HistoryQuery(queryId, userId, vertexId, queryType, queryReceiveTime));
        context.storage().set(QUERY_HISTORY, historyQueries);
        queryExist = true;
      }
    }

    if (!queryExist) {
      historyQueries.add(new HistoryQuery(queryId, userId, vertexId, queryType, queryReceiveTime));
      context.storage().set(QUERY_HISTORY, historyQueries);
    }

    return queryExist;
  }
}
