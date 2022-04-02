package io.github.spycsh.hesse.query;

import io.github.spycsh.hesse.types.HistoryQuery;
import io.github.spycsh.hesse.types.cc.QueryCC;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.scc.QuerySCC;
import io.github.spycsh.hesse.types.minibatch.QueryMiniBatch;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

/**
 * this function handle all the queries from user
 * it will forward the query to the targeted vertex (VertexStorageFn) the state restored from that vertex
 */
public class TemporalQueryHandlerFn implements StatefulFunction {

    private static final ValueSpec<ArrayList<HistoryQuery>> QUERY_HISTORY = ValueSpec.named("queryHistory").withCustomType(Types.QUERY_HISTORY);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.query", "temporal-query-handler");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(TemporalQueryHandlerFn::new)
            .withValueSpecs(QUERY_HISTORY)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.QUERY_MINI_BATCH_TYPE)){
            // send the query info to storage layer with the vertex id that is the query target
            QueryMiniBatch q = message.as(Types.QUERY_MINI_BATCH_TYPE);
            String vertexId = q.getVertexId();
            System.out.printf("[TemporalQueryHandler %s] Query %s of vertex %s with query type %s\n",
                    context.self().id(), q.getQueryId(), vertexId, q.getQueryType());

            /*
                check cache
                if cache has no this query, just forward
                if cache has already receive the query but has no result yet, also forward query
                if cache has this query and already has result, show the result and set queryExist flag true
             */
            boolean queryExist = checkCache(context, q);

            if(!q.getQueryType().equals("mini-batch"))
                throw new IllegalArgumentException("[TemporalQueryHandler] query type should be mini-batch but receive" +
                        "wrong type, check module.yaml or query ingress\n");
            if(!queryExist){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                        .withCustomType(
                                Types.QUERY_MINI_BATCH_TYPE,
                                q)
                        .build());
            }

        } else if(message.is(Types.QUERY_SCC_TYPE)){
            QuerySCC q = message.as(Types.QUERY_SCC_TYPE);
            String vertexId = q.getVertexId();
            System.out.printf("[TemporalQueryHandler %s] Query %s of vertex %s with query type %s\n",
                    context.self().id(), q.getQueryId(), vertexId, q.getQueryType());

            boolean queryExist = checkCache(context, q);

            if(!q.getQueryType().equals("strongly-connected-components"))
                throw new IllegalArgumentException("[TemporalQueryHandler] query type should be mini-batch but receive" +
                        "wrong type, check module.yaml or query ingress\n");
            if(!queryExist){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                        .withCustomType(
                                Types.QUERY_SCC_TYPE,
                                q)
                        .build());
            }

        } else if(message.is(Types.QUERY_CC_TYPE)){
            QueryCC q = message.as(Types.QUERY_CC_TYPE);
            String vertexId = q.getVertexId();
            System.out.printf("[TemporalQueryHandler %s] Query %s of vertex %s with query type %s\n",
                    context.self().id(), q.getQueryId(), vertexId, q.getQueryType());

            boolean queryExist = checkCache(context, q);

            if(!q.getQueryType().equals("connected-components"))
                throw new IllegalArgumentException("[TemporalQueryHandler] query type should be connected-components but receive" +
                        "wrong type, check module.yaml or query ingress\n");
            if(!queryExist){
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                        .withCustomType(
                                Types.QUERY_CC_TYPE,
                                q)
                        .build());
            }

        }

        if(message.is(Types.QUERY_RESULT_TYPE)){
            QueryResult res = message.as(Types.QUERY_RESULT_TYPE);

            ArrayList<HistoryQuery> historyQueries = context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
            for(HistoryQuery hq: historyQueries){
                if(hq.getQueryId().equals(res.getQueryId()) && hq.getQueryId().equals(res.getUserId())){
                    System.out.println(res.getResult());
                    long duration = System.currentTimeMillis() - hq.getQueryReceiveTime();
                    System.out.println("query process time: " + duration + "ms");
                    hq.setResult(res.getResult());
                }
            }
            context.storage().set(QUERY_HISTORY, historyQueries);

        }

        return context.done();
    }

    private boolean checkCache(Context context, QueryMiniBatch q) {
        ArrayList<HistoryQuery> historyQueries = context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
        boolean queryExist = false;
        for(HistoryQuery hq: historyQueries){
            if(hq.getQueryId().equals(q.getQueryId()) && hq.getQueryId().equals(q.getUserId()) && hq.getResult() != null){
                System.out.println(hq.getResult());
                queryExist = true;
                historyQueries.add(new HistoryQuery(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(),
                        System.currentTimeMillis()));
                context.storage().set(QUERY_HISTORY, historyQueries);
            }
        }

        return queryExist;
    }

    private boolean checkCache(Context context, QueryCC q) {
        ArrayList<HistoryQuery> historyQueries = context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
        boolean queryExist = false;
        for(HistoryQuery hq: historyQueries){
            if(hq.getQueryId().equals(q.getQueryId()) && hq.getQueryId().equals(q.getUserId()) && hq.getResult() != null){
                System.out.println(hq.getResult());
                queryExist = true;
                historyQueries.add(new HistoryQuery(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(),
                        System.currentTimeMillis()));
                context.storage().set(QUERY_HISTORY, historyQueries);
            }
        }

        return queryExist;
    }

    private boolean checkCache(Context context, QuerySCC q) {
        ArrayList<HistoryQuery> historyQueries = context.storage().get(QUERY_HISTORY).orElse(new ArrayList<>());
        boolean queryExist = false;
        for(HistoryQuery hq: historyQueries){
            if(hq.getQueryId().equals(q.getQueryId()) && hq.getQueryId().equals(q.getUserId()) && hq.getResult() != null){
                System.out.println(hq.getResult());
                queryExist = true;
                historyQueries.add(new HistoryQuery(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getQueryType(),
                        System.currentTimeMillis()));
                context.storage().set(QUERY_HISTORY, historyQueries);
            }
        }

        return queryExist;
    }
}
