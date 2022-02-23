package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.TemporalWeightedEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

// TODO pass partition number to controller

/**
 * partition by partitionId
 * this file and PartitionManagerFn provide a basic partitioning scheme
 * based on hashing the source id and mod it with the partition number
 * to obtain the partitionId
 * arrange several vertexes into one partition will reduce the communication cost
 */
public class ControllerFn implements StatefulFunction {

    final int partitionNumber = 5;

    // temporal edge
    private static final ValueSpec<TemporalEdge> TEMPORAL_EDGE = ValueSpec.named("temporalEdge").withCustomType(Types.TEMPORAL_EDGE_TYPE);
    // a set of temporal edges
    private static final ValueSpec<HashSet<TemporalEdge>> TEMPORAL_EDGES = ValueSpec.named("temporalEdges").withCustomType(Types.TEMPORAL_EDGES_TYPE);

    public static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "controller");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec
            .builder(TYPE_NAME)
            .withSupplier(ControllerFn::new)
            .withValueSpecs(TEMPORAL_EDGE, TEMPORAL_EDGES)
            .build();

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.TEMPORAL_EDGE_TYPE)){
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

            int partitionIndex = getPartitionIndexByHashing(temporalEdge);

            // send to the specific partition
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.storage", "partitionManager"), String.valueOf(partitionIndex))
                    .withCustomType(
                            Types.TEMPORAL_EDGE_TYPE,
                            temporalEdge)
                    .build());
        }else if(message.is(Types.TEMPORAL_EDGE_WEIGHTED_TYPE)){
            TemporalWeightedEdge temporalWeightedEdge = message.as(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);

            int partitionIndex = getPartitionIndexByHashing(temporalWeightedEdge);
            // send to the specific partition
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.storage", "partitionManager"), String.valueOf(partitionIndex))
                    .withCustomType(
                            Types.TEMPORAL_EDGE_WEIGHTED_TYPE,
                            temporalWeightedEdge)
                    .build());
        }
        return context.done();
    }

    private int getPartitionIndexByHashing(TemporalEdge edge){
        String strToHash = edge.getSrcId() + edge.getDstId();
        int hash = strToHash.hashCode() & Integer.MAX_VALUE;
        return hash % partitionNumber;
    }

    private int getPartitionIndexByHashing(TemporalWeightedEdge edge){
        String strToHash = edge.getSrcId() + edge.getDstId();
        int hash = strToHash.hashCode() & Integer.MAX_VALUE;
        return hash % partitionNumber;
    }
}
