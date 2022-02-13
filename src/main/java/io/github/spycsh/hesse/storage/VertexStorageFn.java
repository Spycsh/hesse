package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.applications.ConnectedComponentsFn;
import io.github.spycsh.hesse.types.TemporalEdge;
import io.github.spycsh.hesse.types.Types;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * storage for one vertex
 * contains basic adjacent neighbours for the vertex
 */
public class VertexStorageFn implements StatefulFunction {

    private static final ValueSpec<Set<Integer>> NEIGHBOURS_VALUE = ValueSpec.named("neighbours").withCustomType(Types.NEIGHBOURS_TYPE);
    private static final ValueSpec<Set<Integer>> BUFFERED_NEIGHBOURS_VALUE = ValueSpec.named("buffered-neighbours").withCustomType(Types.BUFFERED_NEIGHBOURS_VALUE);
    private static final ValueSpec<Long> LAST_MESSAGE_TIME_VALUE = ValueSpec.named("last-message-time-value").withLongType();

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(ConnectedComponentsFn::new)
            .withValueSpecs(NEIGHBOURS_VALUE, BUFFERED_NEIGHBOURS_VALUE, LAST_MESSAGE_TIME_VALUE)
            .build();

    /**
     * pre-defined params
     * TODO can be assigned in config file by user?
     */
    static final int BUFFER_THRESHOLD_SIZE = 5;
    static final int BUFFER_THRESHOLD_TIME = 500;

    List<String> appNames = new ArrayList<String>(){{
        add("connected-components");
    }};

//    private String vertexId;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        Set<Integer> bufferedNeighbours = context.storage().get(BUFFERED_NEIGHBOURS_VALUE).orElse(Collections.emptySet());

        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)){
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

//            this.vertexId = temporalEdge.getSrcId();

            Set<Integer> neighbours = context.storage().get(NEIGHBOURS_VALUE).orElse(Collections.emptySet());
            int neighbourId = Integer.parseInt(temporalEdge.getDstId());
            neighbours.add(neighbourId);
            bufferedNeighbours.add(neighbourId);
        }

        if(bufferedNeighbours.size() >= BUFFER_THRESHOLD_SIZE ||
                (bufferedNeighbours.size() > 0 && getDiffTime(context) > BUFFER_THRESHOLD_TIME)){
            sendBufferedNeighboursToApplications(context, appNames, bufferedNeighbours);
            context.storage().set(BUFFERED_NEIGHBOURS_VALUE, Collections.emptySet());   // clear the buffer
        }

        return context.done();
    }

    // send the buffered neighbours to the specified applications
    private void sendBufferedNeighboursToApplications(Context context, List<String> appNames, Set<Integer> bufferedNeighbours) {
        for(String appName : appNames){
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), context.self().id())
                    .withCustomType(
                            Types.NEIGHBOURS_TYPE,
                            bufferedNeighbours)
                    .build());
        }
    }

    private long getDiffTime(Context context) {
        long curUnixTime = System.currentTimeMillis() / 1000L;
        long lastMessageTime = context.storage().get(LAST_MESSAGE_TIME_VALUE).orElse(-1L);
        if(lastMessageTime == -1){
            context.storage().set(LAST_MESSAGE_TIME_VALUE, curUnixTime);
            return 0L;
        }
        return curUnixTime - lastMessageTime;
    }
}
