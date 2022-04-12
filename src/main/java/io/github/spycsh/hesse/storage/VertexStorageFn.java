package io.github.spycsh.hesse.storage;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.cc.ForwardQueryCC;
import io.github.spycsh.hesse.types.cc.ForwardQueryCCWithState;
import io.github.spycsh.hesse.types.cc.QueryCC;
import io.github.spycsh.hesse.types.cc.QueryCCWithState;
import io.github.spycsh.hesse.types.ingress.TemporalEdge;
import io.github.spycsh.hesse.types.ingress.TemporalWeightedEdge;
import io.github.spycsh.hesse.types.minibatch.ForwardQueryMiniBatch;
import io.github.spycsh.hesse.types.minibatch.ForwardQueryMiniBatchWithState;
import io.github.spycsh.hesse.types.minibatch.QueryMiniBatch;
import io.github.spycsh.hesse.types.minibatch.QueryMiniBatchWithState;
import io.github.spycsh.hesse.types.scc.ForwardQuerySCC;
import io.github.spycsh.hesse.types.scc.ForwardQuerySCCWithState;
import io.github.spycsh.hesse.types.scc.QuerySCC;
import io.github.spycsh.hesse.types.scc.QuerySCCWithState;
import io.github.spycsh.hesse.util.PropertyFileReader;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.apache.flink.statefun.sdk.java.types.SimpleType;
import org.apache.flink.statefun.sdk.java.types.Type;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * partition by vertexId storage function
 */
public class VertexStorageFn implements StatefulFunction {

    private static final ValueSpec<HashSet<Integer>> BUFFERED_NEIGHBOURS_VALUE = ValueSpec.named("bufferedNeighbours").withCustomType(Types.BUFFERED_NEIGHBOURS_VALUE);
    private static final ValueSpec<HashMap<Integer, Double>> BUFFERED_NEIGHBOURS_WEIGHTED_VALUE = ValueSpec.named("bufferedNeighboursWeighted").withCustomType(Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE);

    // during serializing, only serialize the fields that are
    private static final ObjectMapper JSON_OBJ_MAPPER = new ObjectMapper();
    private static final String TYPES_NAMESPACE = "hesse.types";

    private static final ValueSpec<Long> LAST_MESSAGE_TIME_VALUE = ValueSpec.named("lastMessageTime").withLongType();

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

    int storageParadigm = 2;    // default using red black tree

    // valueSpecs of the vertex activities

    private static final ValueSpec<List<VertexActivity>> VERTEX_ACTIVITIES_LIST = ValueSpec.named("vertexActivitiesList").withCustomType(Types.VERTEX_ACTIVITIES_LIST_TYPE);
    private static final ValueSpec<TreeSet<VertexActivity>> VERTEX_ACTIVITIES_RBT = ValueSpec.named("vertexActivitiesRBT").withCustomType(Types.VERTEX_ACTIVITIES_RBT_TYPE);
    private static final ValueSpec<TreeMap<String, ArrayList<VertexActivity>>> VERTEX_ACTIVITIES_BRBT = ValueSpec.named("vertexActivitiesBRBT").withCustomType(Types.VERTEX_ACTIVITIES_BRBT_TYPE);

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

        setStorageParadigm(Integer.parseInt(prop.getProperty("STORAGE_PARADIGM")));
    }

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs(BUFFERED_NEIGHBOURS_VALUE, BUFFERED_NEIGHBOURS_WEIGHTED_VALUE, LAST_MESSAGE_TIME_VALUE,
                    VERTEX_ACTIVITIES_LIST, VERTEX_ACTIVITIES_RBT, VERTEX_ACTIVITIES_BRBT)
            .build();


    public void setStorageParadigm(int storageParadigm) {
        this.storageParadigm = storageParadigm;
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
     *
     * This is specifically used for BRBT
     */
    private static final int EVENT_TIME_INTERVAL = 5;

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
        HashMap<Integer, Double> bufferedNeighboursWeighted =
                context.storage().get(BUFFERED_NEIGHBOURS_WEIGHTED_VALUE).orElse(new HashMap<>());
        HashSet<Integer> bufferedNeighbours = context.storage().get(BUFFERED_NEIGHBOURS_VALUE).orElse(new HashSet<>());

        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)) {
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);
            // store the new activity of into specified batch one single vertex in its context
            // here the default activity type is add
            // should use this to recover to specified state and serve query
            storeActivity(context, temporalEdge);
            int neighbourId = Integer.parseInt(temporalEdge.getDstId());
            bufferedNeighbours.add(neighbourId);
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

        // handle the queries of mini batch
        if(message.is(Types.QUERY_MINI_BATCH_TYPE)){
            QueryMiniBatch q = message.as(Types.QUERY_MINI_BATCH_TYPE);
            System.out.printf("[VertexStorageFn %s] QueryMiniBatch received\n", context.self().id());

            // only send the needed log
            // namely from the beginning to the batch which time T is in
            // remove all the log that is later than the time region that the batch index corresponding to
            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());

            QueryMiniBatchWithState queryWithState = new QueryMiniBatchWithState(
                    q,
                    filteredActivityList
            );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);
        }

        // handle the queries of connected component
        if(message.is(Types.QUERY_SCC_TYPE)){
            QuerySCC q = message.as(Types.QUERY_SCC_TYPE);
            System.out.printf("[VertexStorageFn %s] QueryStronglyConnectedComponent received\n", context.self().id());

            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());
            QuerySCCWithState queryWithState = new QuerySCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryStronglyConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.QUERY_CC_TYPE)){
            QueryCC q = message.as(Types.QUERY_CC_TYPE);
            System.out.printf("[VertexStorageFn %s] QueryConnectedComponent received\n", context.self().id());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            QueryCCWithState queryWithState = new QueryCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_MINI_BATCH_TYPE)){
            ForwardQueryMiniBatch q = message.as(Types.FORWARD_QUERY_MINI_BATCH_TYPE);
            System.out.printf("[VertexStorageFn %s] ForwardQueryMiniBatch received\n", context.self().id());
            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());
            ForwardQueryMiniBatchWithState queryWithState = new ForwardQueryMiniBatchWithState(
                     q,
                     filteredActivityList
             );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_SCC_TYPE)){
            ForwardQuerySCC q = message.as(Types.FORWARD_QUERY_SCC_TYPE);
            System.out.printf("[VertexStorageFn %s] ForwardQuerySCC received\n", context.self().id());
            List<VertexActivity> filteredActivityList = filterActivityListFromBeginningToT(context, q.getT());
            ForwardQuerySCCWithState queryWithState = new ForwardQuerySCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryStronglyConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_CC_TYPE)){
            ForwardQueryCC q = message.as(Types.FORWARD_QUERY_CC_TYPE);
            System.out.printf("[VertexStorageFn %s] ForwardQueryCC received\n", context.self().id());
            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            ForwardQueryCCWithState queryWithState = new ForwardQueryCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryConnectedComponentWithStateToApp(context, queryWithState);
        }

        return context.done();
    }


    private List<VertexActivity> filterActivityListByTimeRegion(Context context, int startT, int endT) {
        switch (storageParadigm){
            case 1:
                List<VertexActivity> vertexActivitiesList = context.storage().get(VERTEX_ACTIVITIES_LIST).orElse(new ArrayList<>());
                List<VertexActivity> vertexActivitiesRes1 = new ArrayList<>();
                for (VertexActivity curActivity : vertexActivitiesList) {
                    if (Integer.parseInt(curActivity.getTimestamp()) <= endT && Integer.parseInt(curActivity.getTimestamp()) >= startT) {
                        vertexActivitiesRes1.add(curActivity);
                    }
                }
                return vertexActivitiesList;
            case 2:
                TreeSet<VertexActivity> vertexActivitiesRBT = context.storage().get(VERTEX_ACTIVITIES_RBT).orElse(
                        new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(o.getTimestamp()))));
                List<VertexActivity> vertexActivitiesRes2 = new ArrayList<>();
                for (VertexActivity curActivity : vertexActivitiesRBT) {
                    if (Integer.parseInt(curActivity.getTimestamp()) > endT) {
                        break;
                    }
                    if (Integer.parseInt(curActivity.getTimestamp()) > startT) {
                        vertexActivitiesRes2.add(curActivity);
                    }
                }
                return vertexActivitiesRes2;
            case 3:
                int batchIndexStart = startT / EVENT_TIME_INTERVAL;
                int batchIndexEnd = endT / EVENT_TIME_INTERVAL;
                TreeMap<String, ArrayList<VertexActivity>> vertexActivitiesBRBT = context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());
                // vertexActivitiesBRBT.keySet().removeIf(key -> Integer.parseInt(key) > batchIndexEnd);
                // vertexActivitiesBRBT.keySet().removeIf(key -> Integer.parseInt(key) < batchIndexStart);
                // return vertexActivitiesBRBT.values().stream().flatMap(ArrayList::stream).collect(Collectors.toList());
                List<VertexActivity> vertexActivitiesRes3 = new ArrayList<>();
                for(int i=batchIndexStart; i<=batchIndexEnd; i++){
                    vertexActivitiesRes3.addAll(vertexActivitiesBRBT.get(String.valueOf(i)));
                }
                return vertexActivitiesRes3;
            default:
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
        }
    }

    private List<VertexActivity> filterActivityListFromBeginningToT(Context context, int T) {
        return filterActivityListByTimeRegion(context, 0, T);
    }

    private void storeActivity(Context context, TemporalEdge temporalEdge) {
        switch (storageParadigm){
            case 1:
                List<VertexActivity> vertexActivitiesList = context.storage().get(VERTEX_ACTIVITIES_LIST).orElse(new ArrayList<>());
                // append into list
                vertexActivitiesList.add(new VertexActivity("add", temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
                context.storage().set(VERTEX_ACTIVITIES_LIST, vertexActivitiesList);
                break;
            case 2:
                TreeSet<VertexActivity> vertexActivitiesRBT = context.storage().get(VERTEX_ACTIVITIES_RBT).orElse(
                        new TreeSet<>(Comparator.comparingInt(o -> Integer.parseInt(o.getTimestamp()))));
                // append into rbt
                vertexActivitiesRBT.add(new VertexActivity("add", temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
                context.storage().set(VERTEX_ACTIVITIES_RBT, vertexActivitiesRBT);
                break;
            case 3:
                TreeMap<String, ArrayList<VertexActivity>> vertexActivitiesBRBT = context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());
                int t = Integer.parseInt(temporalEdge.getTimestamp());
                String batchIndex = String.valueOf(t / EVENT_TIME_INTERVAL);
                ArrayList<VertexActivity> batchActivity = vertexActivitiesBRBT.getOrDefault(batchIndex, new ArrayList<>());
                batchActivity.add(
                        new VertexActivity("add",
                                temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
                vertexActivitiesBRBT.put(batchIndex, batchActivity);
                context.storage().set(VERTEX_ACTIVITIES_BRBT, vertexActivitiesBRBT);
                break;
            default:
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
        }
    }

    // deprecated
//    private void storeActivity(Context context, TemporalWeightedEdge temporalWeightedEdge) {
//        TreeMap<String, ArrayList<VertexActivity>> vertexActivities = context.storage().get(VERTEX_ACTIVITIES).orElse(new TreeMap<>());
//        int batchIndex = Integer.parseInt(temporalWeightedEdge.getTimestamp()) / EVENT_TIME_INTERVAL;
//        ArrayList<VertexActivity> batchActivity = vertexActivities.getOrDefault(String.valueOf(batchIndex), new ArrayList<>());
//        // currently only addition of edges is considered
//        batchActivity.add(
//                new VertexActivity("add",
//                        temporalWeightedEdge.getSrcId(), temporalWeightedEdge.getDstId(),
//                        temporalWeightedEdge.getWeight(), temporalWeightedEdge.getTimestamp()));
//        vertexActivities.put(String.valueOf(batchIndex), batchActivity);
//        context.storage().set(VERTEX_ACTIVITIES, vertexActivities);
//    }

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

    private void sendQueryStronglyConnectedComponentWithStateToApp(Context context, QuerySCCWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), context.self().id())
                .withCustomType(
                        Types.QUERY_SCC_WITH_STATE_TYPE,
                        queryWithState)
                .build());
    }

    private void sendQueryStronglyConnectedComponentWithStateToApp(Context context, ForwardQuerySCCWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "strongly-connected-components"), context.self().id())
                .withCustomType(
                        Types.FORWARD_QUERY_SCC_WITH_STATE_TYPE,
                        queryWithState)
                .build());
    }

    private void sendQueryConnectedComponentWithStateToApp(Context context, QueryCCWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "connected-components"), context.self().id())
                .withCustomType(
                        Types.QUERY_CC_WITH_STATE_TYPE,
                        queryWithState)
                .build());
    }

    private void sendQueryConnectedComponentWithStateToApp(Context context, ForwardQueryCCWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "connected-components"), context.self().id())
                .withCustomType(
                        Types.FORWARD_QUERY_CC_WITH_STATE_TYPE,
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
