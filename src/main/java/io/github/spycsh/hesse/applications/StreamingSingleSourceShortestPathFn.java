package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.VertexShortestPathChange;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * this function serves the streaming tracking of the single source shortest path
 */
public class StreamingSingleSourceShortestPathFn implements StatefulFunction {

    // set of known neighbours with weight from a vertex
    private static final ValueSpec<HashMap<Integer, Double>> NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("neighboursWeighted").withCustomType(Types.NEIGHBOURS_WEIGHTED_TYPE);

    // map of the shortest short path distances of all the nodes (including itself) to it
    private static final ValueSpec<HashMap<String, String>> SHORTEST_PATH_DISTANCES_VALUE =
            ValueSpec.named("shortestPathDistances").withCustomType(Types.SHORTEST_PATH_DISTANCES_TYPE);

    private static final ValueSpec<VertexShortestPathChange> VERTEX_SHORTEST_PATH_CHANGE_VALUE =
            ValueSpec.named("shortestPathChange").withCustomType(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE);

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "streaming-single-source-shortest-path");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(StreamingSingleSourceShortestPathFn::new)
            .withValueSpecs(SHORTEST_PATH_DISTANCES_VALUE, NEIGHBOURS_WEIGHTED_VALUE, VERTEX_SHORTEST_PATH_CHANGE_VALUE)
            .build();

    static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "single-source-shortest-path-changes");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        if(message.is(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE)) {
            final HashMap<Integer, Double> newAddedNeighboursWithWeights = message.as(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE);
            final HashMap<Integer, Double> currentNeighboursWithWeights = getCurrentNeighboursWithWeights(context);
            newAddedNeighboursWithWeights.keySet().removeAll(currentNeighboursWithWeights.keySet());

            // if firstly initialized, add the entry of itself to the shortest path distances map
            // shortestPathDistances: all shortest distances from other source nodes including itself
            HashMap<String, String> shortestPathDistances = context.storage().get(SHORTEST_PATH_DISTANCES_VALUE)
                    .orElse(new HashMap<String, String>(){{
                        put(context.self().id(), "0.0");
                    }});

//            for(Map.Entry<String, String> e:shortestPathDistances.entrySet()){
//                System.out.println("id: "+ context.self().id()+" from source: " + e.getKey() + " weight: "+e.getValue());
//            }

            context.storage().set(SHORTEST_PATH_DISTANCES_VALUE, shortestPathDistances);

            broadcastShortestPathChange(context, newAddedNeighboursWithWeights, shortestPathDistances);

            newAddedNeighboursWithWeights.putAll(currentNeighboursWithWeights);
            context.storage().set(NEIGHBOURS_WEIGHTED_VALUE, newAddedNeighboursWithWeights);

        } else if(message.is(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE)) {
            // get the sender SP map and the weight
            final VertexShortestPathChange shortestPathChange =
                    message.as(Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE);

            HashMap<String, String> sourceShortestPathDistances = shortestPathChange.getShortestPathDistances();
            double weight = shortestPathChange.getWeight();

            // get the self SP map
            HashMap<String, String> shortestPathDistances = context.storage().get(SHORTEST_PATH_DISTANCES_VALUE)
                    .orElse(new HashMap<String, String>(){{
                        put(context.self().id(), "0.0");
                    }});

            // for all the sender source SP entry, compare and update or not update the self SP entry
            for(String srcId : sourceShortestPathDistances.keySet()){
                if(!shortestPathDistances.containsKey(srcId)){
                    updateShortestPathDistance(context, srcId, shortestPathDistances, sourceShortestPathDistances, weight);
                } else {
                    if(Double.parseDouble(sourceShortestPathDistances.get(srcId)) + weight
                            < Double.parseDouble(shortestPathDistances.get(srcId))) {
                        updateShortestPathDistance(context, srcId, shortestPathDistances, sourceShortestPathDistances, weight);
                    }
                }
            }

            outputSingleSourceShortestPathChanges(context, shortestPathDistances);
        }

        return context.done();
    }

    private void updateShortestPathDistance(Context context, String srcId, HashMap<String, String> shortestPathDistances, HashMap<String, String> sourceShortestPathDistances, double weight) {
        shortestPathDistances.put(srcId, String.valueOf(Double.parseDouble(sourceShortestPathDistances.get(srcId)) + weight));
        context.storage().set(SHORTEST_PATH_DISTANCES_VALUE, shortestPathDistances);
        broadcastShortestPathChange(context, getCurrentNeighboursWithWeights(context), shortestPathDistances);
    }

    // for all its neighbours, continue to forward shortest path
    private void broadcastShortestPathChange(Context context,
                                             HashMap<Integer, Double> newAddedNeighboursWithWeights,
                                             HashMap<String, String> shortestPathDistances) {
        for(Map.Entry<Integer, Double> entry: newAddedNeighboursWithWeights.entrySet()){
            context.send(MessageBuilder.forAddress(TYPE_NAME, String.valueOf(entry.getKey()))
                    .withCustomType(
                            Types.VERTEX_SHORTEST_PATH_CHANGE_TYPE,
                            VertexShortestPathChange.create(shortestPathDistances, entry.getValue()))
                    .build());
        }
    }

    private HashMap<Integer, Double> getCurrentNeighboursWithWeights(Context context){
        return context.storage().get(NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());
    }

    private void outputSingleSourceShortestPathChanges(Context context, HashMap<String, String> shortestPathDistances) {
        for(Map.Entry<String, String> e:shortestPathDistances.entrySet()){
            context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                    .withTopic("single-source-shortest-path-changes")
                    .withUtf8Key(context.self().id())
                    .withUtf8Value("id: "+ context.self().id()+" from source: " + e.getKey() + " weight: "+e.getValue())
                    .build());
        }
    }
}
