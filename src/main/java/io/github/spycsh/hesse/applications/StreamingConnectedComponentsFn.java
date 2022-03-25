package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.VertexComponentChange;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * this function serves the streaming tracking of the connected component
 */
public class StreamingConnectedComponentsFn implements StatefulFunction {
    // current component id of a vertex
    private static final ValueSpec<Integer> COMPONENT_ID = ValueSpec.named("componentId").withIntType();

    // set of known neighbours of a vertex
    private static final ValueSpec<HashSet<Integer>> NEIGHBOURS_VALUE = ValueSpec.named("neighbours").withCustomType(Types.NEIGHBOURS_TYPE);



    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "streaming-connected-components");
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(StreamingConnectedComponentsFn::new)
            .withValueSpecs(COMPONENT_ID, NEIGHBOURS_VALUE)
            .build();

    static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "connected-component-changes");

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        String vertexId = context.self().id();
        int vertexIdVal = Integer.parseInt(vertexId);

        if(message.is(Types.BUFFERED_NEIGHBOURS_VALUE)){
            final Set<Integer> newAddedNeighbours = message.as(Types.BUFFERED_NEIGHBOURS_VALUE);

            int currentComponentId = context.storage().get(COMPONENT_ID).orElse(Integer.MAX_VALUE);
            // current component id only can be less than or equals to self id
            // except when it is firstly initialized
            if(currentComponentId > vertexIdVal){
                updateComponentId(context, vertexIdVal, vertexIdVal);
                currentComponentId = vertexIdVal;
            }
            final Set<Integer> currentNeighbours = getCurrentNeighbours(context);

            final HashSet<Integer> neighbourDiff = new HashSet<>(newAddedNeighbours);
            neighbourDiff.removeAll(currentNeighbours);

            // only broadcast to newly added different neighbours
            broadcastVertexConnectedComponentChange(context, Integer.parseInt(vertexId), neighbourDiff, currentComponentId);

            neighbourDiff.addAll(currentNeighbours);
            context.storage().set(NEIGHBOURS_VALUE, neighbourDiff);
        }

        if(message.is(Types.VERTEX_COMPONENT_CHANGE_TYPE)){
            final VertexComponentChange vertexComponentChange = message.as(Types.VERTEX_COMPONENT_CHANGE_TYPE);
            final Set<Integer> currentNeighbours = getCurrentNeighbours(context);

            if(currentNeighbours.contains(vertexComponentChange.getSource())){  // compose bi-direction
                final int componentIdCandidate = vertexComponentChange.getComponentId();
                final int currentComponentId = context.storage().get(COMPONENT_ID).orElse(Integer.MAX_VALUE);

                if(currentComponentId < componentIdCandidate){
                    sendVertexConnectedComponentChange(context, vertexComponentChange.getTarget(), vertexComponentChange.getSource(), currentComponentId);
                } else {
                    updateComponentId(context, vertexComponentChange.getTarget(), componentIdCandidate);
                    currentNeighbours.remove(vertexComponentChange.getSource());    // exclude the sender
                    broadcastVertexConnectedComponentChange(context, vertexComponentChange.getTarget(), currentNeighbours, componentIdCandidate);
                }
            }
        }

        return context.done();
    }

    private void updateComponentId(Context context, int vertexId, int candidateId) {
        context.storage().set(COMPONENT_ID, candidateId);
        // TODO query the component changes
        outputConnectedComponentChange(context, vertexId, candidateId);
    }

    private void broadcastVertexConnectedComponentChange(Context context, int srcId, Set<Integer> neighbours, int componentId) {
        for(int neighbour: neighbours){
            sendVertexConnectedComponentChange(context, srcId, neighbour, componentId);
        }
    }

    private void sendVertexConnectedComponentChange(Context context, int source, int target, int currentComponentId){
        final VertexComponentChange vertexComponentChange = VertexComponentChange.create(source, target, currentComponentId);
        context.send(MessageBuilder.forAddress(TYPE_NAME, String.valueOf(target))
                .withCustomType(
                        Types.VERTEX_COMPONENT_CHANGE_TYPE,
                        vertexComponentChange)
                .build());
    }

    private Set<Integer> getCurrentNeighbours(Context context){
        return context.storage().get(NEIGHBOURS_VALUE).orElse(new HashSet<>());
    }

    private void outputConnectedComponentChange(Context context, int vertexId, int componentId) {
        context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
                .withTopic("connected-component-changes")
                .withUtf8Key(String.valueOf(vertexId))
                .withUtf8Value(String.format("Vertex %s belongs to component %s.", vertexId, componentId))
                .build());
    }

}
