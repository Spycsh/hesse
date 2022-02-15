package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.VertexShortestPathChange;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class SingleSourceShortestPathFn implements StatefulFunction {

    // set of known neighbours with weight from a vertex
    private static final ValueSpec<ConcurrentHashMap<Integer, Double>> NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("neighboursWeighted").withCustomType(Types.NEIGHBOURS_WEIGHTED_TYPE);

    // map of the shortest short path distances of all the nodes (including itself) to it
    private static final ValueSpec<ConcurrentHashMap<Integer, Double>> SHORTEST_PATH_DISTANCES_VALUE =
            ValueSpec.named("shortestPathDistances").withCustomType(Types.SHORTEST_PATH_DISTANCES_TYPE);

    private static final ValueSpec<VertexShortestPathChange> VERTEX_SHORTEST_PATH_CHANGE_VALUE =
            ValueSpec.named("shortestPathChange").withCustomType(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "connected-components");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(ConnectedComponentsFn::new)
            .withValueSpecs(SHORTEST_PATH_DISTANCES_VALUE, NEIGHBOURS_WEIGHTED_VALUE)
            .build();

//    static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "single-shortest-path");


    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE)) {
            final ConcurrentHashMap<Integer, Double> newAddedNeighboursWithWeights = message.as(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE);
            final ConcurrentHashMap<Integer, Double> currentNeighboursWithWeights = getCurrentNeighboursWithWeights(context);
            newAddedNeighboursWithWeights.keySet().removeAll(currentNeighboursWithWeights.keySet());

            // if firstly initialized, add the entry of itself to the shortest path distances map
            // shortestPathDistances: all shortest distances from other source nodes including itself
            ConcurrentHashMap<Integer, Double> shortestPathDistances = context.storage().get(SHORTEST_PATH_DISTANCES_VALUE)
                    .orElse(new ConcurrentHashMap<Integer, Double>(){{
                        put(Integer.parseInt(context.self().id()), 0.0);
                    }});

            context.storage().set(SHORTEST_PATH_DISTANCES_VALUE, shortestPathDistances);

            broadcastShortestPathChange(context, newAddedNeighboursWithWeights, shortestPathDistances);

            newAddedNeighboursWithWeights.putAll(currentNeighboursWithWeights);
            context.storage().set(NEIGHBOURS_WEIGHTED_VALUE, newAddedNeighboursWithWeights);

        } else if(message.is(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE)) {
            // get the sender SP map and the weight
            final VertexShortestPathChange shortestPathChange =
                    message.as(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE);

            ConcurrentHashMap<Integer, Double> sourceShortestPathDistances = shortestPathChange.getShortestPathDistances();
            double weight = shortestPathChange.getWeight();

            // get the self SP map
            ConcurrentHashMap<Integer, Double> shortestPathDistances = context.storage().get(SHORTEST_PATH_DISTANCES_VALUE)
                    .orElse(new ConcurrentHashMap<Integer, Double>(){{
                        put(Integer.parseInt(context.self().id()), 0.0);
                    }});

            // for all the sender source SP entry, compare and update or not update the self SP entry
            for(int srcId : sourceShortestPathDistances.keySet()){
                if(!shortestPathDistances.containsKey(srcId)){
                    updateShortestPathDistance(context, srcId, shortestPathDistances, sourceShortestPathDistances, weight);
                } else {
                    if(sourceShortestPathDistances.get(srcId) + weight < shortestPathDistances.get(srcId)) {
                        updateShortestPathDistance(context, srcId, shortestPathDistances, sourceShortestPathDistances, weight);
                    }
                }
            }
        }

        return context.done();
    }

    private void updateShortestPathDistance(Context context, int srcId, ConcurrentHashMap<Integer, Double> shortestPathDistances, ConcurrentHashMap<Integer, Double> sourceShortestPathDistances, double weight) {
        shortestPathDistances.put(srcId, sourceShortestPathDistances.get(srcId) + weight);
        context.storage().set(SHORTEST_PATH_DISTANCES_VALUE, shortestPathDistances);
        broadcastShortestPathChange(context, getCurrentNeighboursWithWeights(context), shortestPathDistances);
    }


    private void broadcastShortestPathChange(Context context,
                                             ConcurrentHashMap<Integer, Double> newAddedNeighboursWithWeights,
                                             ConcurrentHashMap<Integer, Double> shortestPathDistances) {
        for(int neighbourId : newAddedNeighboursWithWeights.keySet()) {
            double weight = newAddedNeighboursWithWeights.get(neighbourId);
            context.send(MessageBuilder.forAddress(TYPE_NAME, String.valueOf(neighbourId))
                    .withCustomType(
                            Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE,
                            VertexShortestPathChange.create(shortestPathDistances, weight))
                    .build());
        }
    }

    private ConcurrentHashMap<Integer, Double> getCurrentNeighboursWithWeights(Context context){
        return context.storage().get(NEIGHBOURS_WEIGHTED_VALUE).orElse(new ConcurrentHashMap<>());
    }
}
