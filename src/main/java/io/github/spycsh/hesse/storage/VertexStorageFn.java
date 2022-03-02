package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.util.PropertyFileReader;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * partition by vertexId
 */
public class VertexStorageFn implements StatefulFunction {

    private static final ValueSpec<HashSet<Integer>> NEIGHBOURS_VALUE = ValueSpec.named("neighbours").withCustomType(Types.NEIGHBOURS_TYPE);
    private static final ValueSpec<HashSet<Integer>> BUFFERED_NEIGHBOURS_VALUE = ValueSpec.named("bufferedNeighbours").withCustomType(Types.BUFFERED_NEIGHBOURS_VALUE);

    private static final ValueSpec<HashMap<Integer, Double>> NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("neighboursWeighted").withCustomType(Types.NEIGHBOURS_WEIGHTED_TYPE);
    private static final ValueSpec<HashMap<Integer, Double>> BUFFERED_NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("bufferedNeighboursWeighted").withCustomType(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE);

    private static final ValueSpec<TreeMap<Integer, ArrayList<VertexActivity>>> VERTEX_ACTIVITIES = ValueSpec.named("vertexActivities").withCustomType(Types.VERTEX_ACTIVITIES);


    private static final ValueSpec<Long> LAST_MESSAGE_TIME_VALUE = ValueSpec.named("lastMessageTime").withLongType();

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs(NEIGHBOURS_VALUE, NEIGHBOURS_WEIGHTED_VALUE, BUFFERED_NEIGHBOURS_VALUE, BUFFERED_NEIGHBOURS_WEIGHTED_VALUE, LAST_MESSAGE_TIME_VALUE, VERTEX_ACTIVITIES)
            .build();

    int BUFFER_THRESHOLD_SIZE;
    int BUFFER_THRESHOLD_TIME;

    public void setBufferThresholdSize(int size){
        this.BUFFER_THRESHOLD_SIZE = size;
    }

    public void setBufferThresholdTime(int time){
        this.BUFFER_THRESHOLD_TIME = time;
    }

    List<String> unweightedAppNames = new ArrayList<>();
    List<String> weightedAppNames = new ArrayList<>();

    Properties prop;

    {
        try {
            prop = PropertyFileReader.readPropertyFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
        setBufferThresholdSize(Integer.parseInt(prop.getProperty("BUFFER_THRESHOLD_SIZE")));
        setBufferThresholdTime(Integer.parseInt(prop.getProperty("BUFFER_THRESHOLD_TIME")));

        setUnWeightedAppNames(prop.getProperty("UNWEIGHTED_APP_NAMES"));
        setWeightedAppNames(prop.getProperty("WEIGHTED_APP_NAMES"));
    }

    public void setUnWeightedAppNames(String appNames) {
        this.unweightedAppNames.addAll(Arrays.asList(appNames.split(",")));
    }

    public void setWeightedAppNames(String appNames) {
        this.weightedAppNames.addAll(Arrays.asList(appNames.split(",")));
    }

    /**
     * A pre-config event time interval that used to separate
     * the whole sequence of temporal edges into batches
     * e.g. there are three records with timestamp 0, 5, 7
     * and the event time interval is 5
     * then it will have two batches [0, 5), [5, 10)
     * 0 will be in the first batch while 5 and 7 will be in the second one
     */
    private static final int EVENT_TIME_INTERVAL = 5;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        System.out.println(SPEC.knownValues());
        HashMap<Integer, Double> bufferedNeighboursWeighted =
                context.storage().get(BUFFERED_NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());
        HashSet<Integer> bufferedNeighbours = context.storage().get(BUFFERED_NEIGHBOURS_VALUE).orElse(new HashSet<>());


        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)) {
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

            // store the new activity of into specified batch one single vertex in its context
            // here the default activity type is add
            // should use this to recover to specified state and serve query
            // TODO persistence, event time batch checkpointing
            storeActivity(context, temporalEdge);

//            HashSet<Integer> neighbours = context.storage().get(NEIGHBOURS_VALUE).orElse(new HashSet<>());
            int neighbourId = Integer.parseInt(temporalEdge.getDstId());
//            neighbours.add(neighbourId);
//            context.storage().set(NEIGHBOURS_VALUE, neighbours);
            bufferedNeighbours.add(neighbourId);
        } else if(message.is(Types.TEMPORAL_EDGE_WEIGHTED_TYPE)) {
            TemporalWeightedEdge temporalWeightedEdge = message.as(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);
            // TODO persistence, event time batch checkpointing
            storeActivity(context, temporalWeightedEdge);
//            HashMap<Integer, Double> neighboursWeighted = context.storage().get(NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());
            int neighbourId = Integer.parseInt(temporalWeightedEdge.getDstId());
            // System.out.println(temporalWeightedEdge.getSrcId() + "->" + temporalWeightedEdge.getDstId());
            double weight = Double.parseDouble(temporalWeightedEdge.getWeight());
//            neighboursWeighted.put(neighbourId, weight);
//            context.storage().set(NEIGHBOURS_WEIGHTED_VALUE, neighboursWeighted);
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

        // handle the queries
        if(message.is(Types.QUERY_MINI_BATCH_TYPE)){
            QueryMiniBatch q = message.as(Types.QUERY_MINI_BATCH_TYPE);

            // only send the needed log
            // namely from the beginning to the batch which time T is in
            // TODO restore from checkpoints
            // remove all the log that is later than the time region that the batch index corresponding to
            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());

            QueryMiniBatchWithState queryWithState = new QueryMiniBatchWithState(
                    q,
                    filteredActivityList
            );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_MINI_BATCH_TYPE)){
            ForwardQueryMiniBatch q = message.as(Types.FORWARD_QUERY_MINI_BATCH_TYPE);

            String sourceId = q.getSource();

            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());

            ForwardQueryMiniBatchWithState queryWithState = new ForwardQueryMiniBatchWithState(
                     sourceId,
                     q,
                     filteredActivityList
             );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);

        }

        return context.done();
    }

    private List<VertexActivity> filterActivityListFromBeginningToT(Context context, int T) {
        int batchIndex = T / EVENT_TIME_INTERVAL;
        TreeMap<Integer, ArrayList<VertexActivity>> vertexActivities = context.storage().get(VERTEX_ACTIVITIES).orElse(new TreeMap<>());
        vertexActivities.keySet().removeIf(key -> key > batchIndex);
        return vertexActivities.values().stream().flatMap(ArrayList::stream).collect(Collectors.toList());
    }

    private void storeActivity(Context context, TemporalEdge temporalEdge) {
        TreeMap<Integer, ArrayList<VertexActivity>> vertexActivities = context.storage().get(VERTEX_ACTIVITIES).orElse(new TreeMap<>());
        int batchIndex = Integer.parseInt(temporalEdge.getTimestamp()) / EVENT_TIME_INTERVAL;
        ArrayList<VertexActivity> batchActivity = vertexActivities.getOrDefault(batchIndex, new ArrayList<>());
        // currently only addition of edges is considered
        batchActivity.add(
                new VertexActivity("add",
                        temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
        vertexActivities.put(batchIndex, batchActivity);
        context.storage().set(VERTEX_ACTIVITIES, vertexActivities);
    }

    private void storeActivity(Context context, TemporalWeightedEdge temporalWeightedEdge) {
        TreeMap<Integer, ArrayList<VertexActivity>> vertexActivities = context.storage().get(VERTEX_ACTIVITIES).orElse(new TreeMap<>());
        int batchIndex = Integer.parseInt(temporalWeightedEdge.getTimestamp()) / EVENT_TIME_INTERVAL;
        ArrayList<VertexActivity> batchActivity = vertexActivities.getOrDefault(batchIndex, new ArrayList<>());
        // currently only addition of edges is considered
        batchActivity.add(
                new VertexActivity("add",
                        temporalWeightedEdge.getSrcId(), temporalWeightedEdge.getDstId(),
                        temporalWeightedEdge.getWeight(), temporalWeightedEdge.getTimestamp()));
        vertexActivities.put(batchIndex, batchActivity);
        context.storage().set(VERTEX_ACTIVITIES, vertexActivities);
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

    private void sendQueryMiniBatchWithStateToApp(Context context, QueryMiniBatchWithState queryWithState) {
        context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), context.self().id())
                        .withCustomType(
                                Types.QUERY_MINI_BATCH_WITH_STATE_TYPE,
                                queryWithState)
                        .build());
    }

    private void sendQueryMiniBatchWithStateToApp(Context context, ForwardQueryMiniBatchWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "mini-batch"), context.self().id())
                .withCustomType(
                        Types.FORWARD_QUERY_MINI_BATCH_WITH_STATE_TYPE,
                        queryWithState)
                .build());
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
