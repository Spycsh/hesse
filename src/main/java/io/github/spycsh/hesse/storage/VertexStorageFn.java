package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.applications.ConnectedComponentsFn;
import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.TemporalWeightedEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * storage for one vertex
 * contains basic adjacent neighbours for the vertex
 */
public class VertexStorageFn implements StatefulFunction {

    private static final ValueSpec<HashSet<Integer>> NEIGHBOURS_VALUE = ValueSpec.named("neighbours").withCustomType(Types.NEIGHBOURS_TYPE);
    private static final ValueSpec<HashSet<Integer>> BUFFERED_NEIGHBOURS_VALUE = ValueSpec.named("bufferedNeighbours").withCustomType(Types.BUFFERED_NEIGHBOURS_VALUE);

    private static final ValueSpec<HashMap<Integer, Double>> NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("neighboursWeighted").withCustomType(Types.NEIGHBOURS_WEIGHTED_TYPE);
    private static final ValueSpec<HashMap<Integer, Double>> BUFFERED_NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("bufferedNeighboursWeighted").withCustomType(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE);

    // TODO may hold a list for different sources
    private static final ValueSpec<Long> LAST_MESSAGE_TIME_VALUE = ValueSpec.named("lastMessageTime").withLongType();

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs(NEIGHBOURS_VALUE, NEIGHBOURS_WEIGHTED_VALUE, BUFFERED_NEIGHBOURS_VALUE, BUFFERED_NEIGHBOURS_WEIGHTED_VALUE, LAST_MESSAGE_TIME_VALUE)
            .build();

    /**
     * pre-defined params
     * TODO can be assigned in config file by user?
     */
    static final int BUFFER_THRESHOLD_SIZE = 5;
    static final int BUFFER_THRESHOLD_TIME = 500;   // in milliseconds

    List<String> unweightedAppNames = new ArrayList<String>(){{
        add("connected-components");
    }};

    List<String> weightedAppNames = new ArrayList<String>(){{
        add("single-source-shortest-path");
    }};

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        HashSet<Integer> bufferedNeighbours = context.storage().get(BUFFERED_NEIGHBOURS_VALUE).orElse(new HashSet<>());
        HashMap<Integer, Double> bufferedNeighboursWeighted =
                context.storage().get(BUFFERED_NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());

        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)) {
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

            HashSet<Integer> neighbours = context.storage().get(NEIGHBOURS_VALUE).orElse(new HashSet<>());
            int neighbourId = Integer.parseInt(temporalEdge.getDstId());
            neighbours.add(neighbourId);
            context.storage().set(NEIGHBOURS_VALUE, neighbours);
            bufferedNeighbours.add(neighbourId);
        } else if(message.is(Types.TEMPORAL_EDGE_WEIGHTED_TYPE)) {
            TemporalWeightedEdge temporalWeightedEdge = message.as(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);

            HashMap<Integer, Double> neighboursWeighted = context.storage().get(NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());
            int neighbourId = Integer.parseInt(temporalWeightedEdge.getDstId());
            // System.out.println(temporalWeightedEdge.getSrcId() + "->" + temporalWeightedEdge.getDstId());
            double weight = Double.parseDouble(temporalWeightedEdge.getWeight());
            neighboursWeighted.put(neighbourId, weight);
            context.storage().set(NEIGHBOURS_WEIGHTED_VALUE, neighboursWeighted);
            bufferedNeighboursWeighted.put(neighbourId, weight);
        }

        if(bufferedNeighbours.size() >= BUFFER_THRESHOLD_SIZE ||
                (bufferedNeighbours.size() > 0 && getDiffTime(context) > BUFFER_THRESHOLD_TIME)){
            sendBufferedNeighboursToApplications(context, bufferedNeighbours);
            context.storage().set(BUFFERED_NEIGHBOURS_VALUE, new HashSet<>());   // clear the buffer
        }

        if(bufferedNeighboursWeighted.size() >= BUFFER_THRESHOLD_SIZE ||
                (bufferedNeighboursWeighted.size() > 0 && getDiffTime(context) > BUFFER_THRESHOLD_TIME)){
            sendBufferedNeighboursToApplications(context, bufferedNeighboursWeighted);
            context.storage().set(BUFFERED_NEIGHBOURS_WEIGHTED_VALUE, new HashMap<>());   // clear buffer
        }

        return context.done();
    }

    // send the buffered neighbours to the specified UNWEIGHTED applications
    private void sendBufferedNeighboursToApplications(Context context, HashSet<Integer> bufferedNeighbours) {
        for(String appName : unweightedAppNames){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), context.self().id())
                    .withCustomType(
                            Types.BUFFERED_NEIGHBOURS_VALUE,
                            bufferedNeighbours)
                    .build());
        }
    }

    // send the buffered neighbours to the specified WEIGHTED applications
    private void sendBufferedNeighboursToApplications(Context context, HashMap<Integer, Double> bufferedNeighboursWeighted) {
        for(String appName : weightedAppNames){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), context.self().id())
                    .withCustomType(
                            Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE,
                            bufferedNeighboursWeighted)
                    .build());
        }
    }

    private long getDiffTime(Context context) {
        long curUnixTime = System.currentTimeMillis();
        long lastMessageTime = context.storage().get(LAST_MESSAGE_TIME_VALUE).orElse(-1L);
        if(lastMessageTime == -1){
            context.storage().set(LAST_MESSAGE_TIME_VALUE, curUnixTime);
            return 0L;
        }
        return curUnixTime - lastMessageTime;
    }
}
