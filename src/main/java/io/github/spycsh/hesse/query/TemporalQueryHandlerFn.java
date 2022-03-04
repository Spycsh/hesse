package io.github.spycsh.hesse.query;

import io.github.spycsh.hesse.storage.VertexStorageFn;
import io.github.spycsh.hesse.types.QueryMiniBatch;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.Context;
import org.apache.flink.statefun.sdk.java.StatefulFunction;
import org.apache.flink.statefun.sdk.java.StatefulFunctionSpec;
import org.apache.flink.statefun.sdk.java.TypeName;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.concurrent.CompletableFuture;

/**
 * this function handle all the queries from user
 * it will forward the query to the targeted vertex (VertexStorageFn) the state restored from that vertex
 */
public class TemporalQueryHandlerFn implements StatefulFunction {

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.query", "temporal-query-handler");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(TemporalQueryHandlerFn::new)
            .withValueSpecs()
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.QUERY_MINI_BATCH_TYPE)){
            // send the query info to storage layer with the vertex id that is the query target
            QueryMiniBatch q = message.as(Types.QUERY_MINI_BATCH_TYPE);
            String vertexId = q.getVertexId();
            System.out.println("Query " + q.getQueryId() + " of vertex " + q.getVertexId() + " "  + q.getQueryType());
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), vertexId)
                    .withCustomType(
                            Types.QUERY_MINI_BATCH_TYPE,
                            q)
                    .build());
        }

        return context.done();
    }
}
