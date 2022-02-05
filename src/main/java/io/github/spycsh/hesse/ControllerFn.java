package io.github.spycsh.hesse;

import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

// TODO pass partition number to controller

public class ControllerFn implements StatefulFunction {

    final int partitionNumber = 5;

    // temporal edge
    private static final ValueSpec<TemporalEdge> TEMPORAL_EDGE = ValueSpec.named("temporalEdge").withCustomType(Types.TEMPORAL_EDGE_TYPE);
    // a set of temporal edges
    private static final ValueSpec<Set<TemporalEdge>> TEMPORAL_EDGES = ValueSpec.named("temporalEdges").withCustomType(Types.TEMPORAL_EDGES_TYPE);

    public static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.fns", "controller");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec
            .builder(TYPE_NAME)
            .withSupplier(ControllerFn::new)
            .withValueSpecs(TEMPORAL_EDGE, TEMPORAL_EDGES)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.TEMPORAL_EDGE_TYPE)){
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);
            String strToHash = temporalEdge.getSrcId() + temporalEdge.getDstId();
            // the result should be the hashcode without negative sign
            int hash = strToHash.hashCode() & Integer.MAX_VALUE;
            // get the partition that this temporal edge to be sent to
            int partitionIndex = hash % partitionNumber;

            // send to the specific partition
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.fns", "partitionManager"), String.valueOf(partitionIndex))
                    .withCustomType(
                            Types.TEMPORAL_EDGE_TYPE,
                            temporalEdge)
                    .build());
        }
        return context.done();
    }
}
