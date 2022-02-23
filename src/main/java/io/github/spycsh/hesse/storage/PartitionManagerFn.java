package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.types.PartitionConfig;
import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.TemporalWeightedEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class PartitionManagerFn implements StatefulFunction {

    // every partition manager must hold a partition id
    private static final ValueSpec<String> PARTITION_ID = ValueSpec.named("partitionId").withUtf8StringType();
    // temporal edge
    private static final ValueSpec<TemporalEdge> TEMPORAL_EDGE = ValueSpec.named("temporalEdge").withCustomType(Types.TEMPORAL_EDGE_TYPE);
    // a set of temporal edges
    private static final ValueSpec<HashSet<TemporalEdge>> TEMPORAL_EDGES =
            ValueSpec.named("temporalEdges").withCustomType(Types.TEMPORAL_EDGES_TYPE);

    private static final ValueSpec<TemporalWeightedEdge> TEMPORAL_WEIGHTED_EDGE = ValueSpec.named("temporalWeightedEdge").withCustomType(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);
    private static final ValueSpec<HashSet<TemporalWeightedEdge>> TEMPORAL_WEIGHTED_EDGES =
            ValueSpec.named("temporalWeightedEdges").withCustomType(Types.TEMPORAL_EDGES_WEIGHTED_TYPE);


    static final int BUFFER_THRESHOLD_SIZE = 5;
    static final int BUFFER_THRESHOLD_TIME = 500;   // in milliseconds

    List<String> unweightedAppNames = new ArrayList<String>(){{
        add("connected-components");
    }};

    List<String> weightedAppNames = new ArrayList<String>(){{
        add("single-source-shortest-path");
    }};

    public static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "partitionManager");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec
            .builder(TYPE_NAME)
            .withSupplier(PartitionManagerFn::new)
            .withValueSpecs(PARTITION_ID, TEMPORAL_EDGE, TEMPORAL_EDGES, TEMPORAL_WEIGHTED_EDGE, TEMPORAL_WEIGHTED_EDGES)
            .build();

    static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "partition-edges");

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
            HashSet<TemporalEdge> temporalEdges = context.storage().get(TEMPORAL_EDGES).orElse(new HashSet<>());
            // get the temporal edge that needed to be added
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);
            temporalEdges.add(temporalEdge);
            context.storage().set(TEMPORAL_EDGES, temporalEdges);

            outputTemporalEdgeChange(context, temporalEdge);
            sendNeighbourToApplications(context, temporalEdge);
        } else if(message.is(Types.TEMPORAL_EDGE_WEIGHTED_TYPE)){
            // get current set of temporal edges
            HashSet<TemporalWeightedEdge> temporalWeightedEdges = context.storage().get(TEMPORAL_WEIGHTED_EDGES).orElse(new HashSet<>());
            // get the temporal edge that needed to be added
            TemporalWeightedEdge temporalWeightedEdge = message.as(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);
            temporalWeightedEdges.add(temporalWeightedEdge);
            context.storage().set(TEMPORAL_WEIGHTED_EDGES, temporalWeightedEdges);

            outputTemporalEdgeChange(context, temporalWeightedEdge);
            sendNeighbourToApplications(context, temporalWeightedEdge);
        }

        return context.done();
    }

    private void outputTemporalEdgeChange(Context context, TemporalEdge edge){
        String partitionId = context.self().id();
        context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                .withTopic("partition-edges")
                .withUtf8Key(String.valueOf(partitionId))
                .withUtf8Value(String.format(
                        "partition %s: Newly added edge %s -> %s at time %s",
                        partitionId, edge.getSrcId(), edge.getDstId(), edge.getTimestamp()))
                .build());
    }

    private void outputTemporalEdgeChange(Context context, TemporalWeightedEdge edge){
        String partitionId = context.self().id();
        context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                .withTopic("partition-edges")
                .withUtf8Key(String.valueOf(partitionId))
                .withUtf8Value(String.format(
                        "partition %s: Newly added edge %s -> %s at time %s with weight %s",
                        partitionId, edge.getSrcId(), edge.getDstId(), edge.getTimestamp(), edge.getWeight()))
                .build());
    }

    // send the buffered neighbours to the specified UNWEIGHTED applications
    private void sendNeighbourToApplications(Context context, TemporalEdge edge) {
        // send one record buffer at a time
        HashSet<Integer> bufferedNeighbours = new HashSet<Integer>(){{
            add(Integer.parseInt(edge.getDstId()));
        }};

        for(String appName : unweightedAppNames){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), edge.getSrcId())
                    .withCustomType(
                            Types.BUFFERED_NEIGHBOURS_VALUE,
                            bufferedNeighbours)
                    .build());
        }
    }

    // send the buffered neighbours to the specified WEIGHTED applications
    private void sendNeighbourToApplications(Context context, TemporalWeightedEdge edge) {
        // send one record buffer at a time
        HashMap<Integer, Double> bufferedWeightedNeighbours = new HashMap<Integer, Double>(){{
            put(Integer.parseInt(edge.getDstId()), Double.parseDouble(edge.getWeight()));
        }};
        for(String appName : weightedAppNames){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), edge.getSrcId())
                    .withCustomType(
                            Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE,
                            bufferedWeightedNeighbours)
                    .build());
        }
    }
}
