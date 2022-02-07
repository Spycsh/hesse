package io.github.spycsh.hesse;

import io.github.spycsh.hesse.types.PartitionConfig;
import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public class PartitionManagerFn implements StatefulFunction {


    // every partition manager must hold a partition id
    private static final ValueSpec<String> PARTITION_ID = ValueSpec.named("partitionId").withUtf8StringType();
    // temporal edge
    private static final ValueSpec<TemporalEdge> TEMPORAL_EDGE = ValueSpec.named("temporalEdge").withCustomType(Types.TEMPORAL_EDGE_TYPE);
    // a set of temporal edges
    private static final ValueSpec<Set<TemporalEdge>> TEMPORAL_EDGES = ValueSpec.named("temporalEdges").withCustomType(Types.TEMPORAL_EDGES_TYPE);


    public static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.fns", "partitionManager");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec
            .builder(TYPE_NAME)
            .withSupplier(PartitionManagerFn::new)
            .withValueSpecs(PARTITION_ID, TEMPORAL_EDGE, TEMPORAL_EDGES)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.PARTITION_CONFIG_TYPE)){
            PartitionConfig config = message.as(Types.PARTITION_CONFIG_TYPE);
            context.storage().set(PARTITION_ID, config.getPartitionId());
        }

        // TODO OPERATION ADD, DELETE, EDIT
        // if a partition receives the temporal edge route to it
        // it will store in the internal buffer
        if(message.is(Types.TEMPORAL_EDGE_TYPE)){
            // get current set of temporal edges
            Set<TemporalEdge> temporalEdges = context.storage().get(TEMPORAL_EDGES).orElse(new HashSet<>());
            // get the temporal edge that needed to be added
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

            context.storage().set(TEMPORAL_EDGES, temporalEdges);
        }

        return context.done();
    }
}
