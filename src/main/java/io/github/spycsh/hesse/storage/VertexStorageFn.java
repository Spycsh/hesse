package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.applications.ConnectedComponentsFn;
import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.cc.ForwardQueryCC;
import io.github.spycsh.hesse.types.cc.ForwardQueryCCWithState;
import io.github.spycsh.hesse.types.cc.QueryCC;
import io.github.spycsh.hesse.types.cc.QueryCCWithState;
import io.github.spycsh.hesse.types.ingress.TemporalEdge;
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
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * partition by vertexId storage function
 */
public class VertexStorageFn implements StatefulFunction {

    private static final ValueSpec<Long> LAST_MESSAGE_TIME_VALUE = ValueSpec.named("lastMessageTime").withLongType();
    // record increment of activities
    private static final ValueSpec<Long> ACTIVITY_INDEX = ValueSpec.named("activityIndex").withLongType();

    int storageParadigm = 2;    // default paradigm

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
    private int eventTimeInterval = 5;

    // valueSpecs of the vertex activities

    private static final ValueSpec<List<VertexActivity>> VERTEX_ACTIVITIES_LIST = ValueSpec.named("vertexActivitiesList").withCustomType(Types.VERTEX_ACTIVITIES_LIST_TYPE);
    private static final ValueSpec<PriorityQueue<VertexActivity>> VERTEX_ACTIVITIES_PQ = ValueSpec.named("vertexActivitiesPQ").withCustomType(Types.VERTEX_ACTIVITIES_PQ_TYPE);
    private static final ValueSpec<TreeMap<String, List<VertexActivity>>> VERTEX_ACTIVITIES_BRBT = ValueSpec.named("vertexActivitiesBRBT").withCustomType(Types.VERTEX_ACTIVITIES_BRBT_TYPE);

    Properties prop;

    {
        try {
            prop = PropertyFileReader.readPropertyFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        setStorageParadigm(Integer.parseInt(prop.getProperty("STORAGE_PARADIGM")));
        setEventTimeInterval(Integer.parseInt(prop.getProperty("EVENT_TIME_INTERVAL")));
    }

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs(VERTEX_ACTIVITIES_LIST, VERTEX_ACTIVITIES_PQ, VERTEX_ACTIVITIES_BRBT, ACTIVITY_INDEX)
            .build();


    public void setStorageParadigm(int storageParadigm) {
        this.storageParadigm = storageParadigm;
    }
    public void setEventTimeInterval(int eventTimeInterval) {
        this.eventTimeInterval = eventTimeInterval;
    }

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(VertexStorageFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)) {
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);
            // store the new activity of into specified batch one single vertex in its context
            // here the default activity type is add
            // should use this to recover to specified state and serve query
            // TODO transaction here
            storeActivity(context, temporalEdge);

            long activityIndex = context.storage().get(ACTIVITY_INDEX).orElse(1L);
            LOGGER.debug("[VertexStorageFn {}] process activity {}", context.self().id(), activityIndex);
            context.storage().set(ACTIVITY_INDEX, activityIndex + 1);
        }

        // handle the queries of mini batch
        if(message.is(Types.QUERY_MINI_BATCH_TYPE)){
            QueryMiniBatch q = message.as(Types.QUERY_MINI_BATCH_TYPE);
            LOGGER.info("[VertexStorageFn {}] QueryMiniBatch received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            // only send the needed log
            // namely from the beginning to the batch which time T is in
            // remove all the log that is later than the time region that the batch index corresponding to
            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());

            QueryMiniBatchWithState queryWithState = new QueryMiniBatchWithState(
                    q,
                    filteredActivityList
            );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);
        }

        // handle the queries of connected component
        if(message.is(Types.QUERY_SCC_TYPE)){
            QuerySCC q = message.as(Types.QUERY_SCC_TYPE);
            LOGGER.info("[VertexStorageFn {}] QueryStronglyConnectedComponent received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());


            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            QuerySCCWithState queryWithState = new QuerySCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryStronglyConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.QUERY_CC_TYPE)){
            QueryCC q = message.as(Types.QUERY_CC_TYPE);
            LOGGER.info("[VertexStorageFn {}] QueryConnectedComponent received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            QueryCCWithState queryWithState = new QueryCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_MINI_BATCH_TYPE)){
            ForwardQueryMiniBatch q = message.as(Types.FORWARD_QUERY_MINI_BATCH_TYPE);
            LOGGER.info("[VertexStorageFn {}] ForwardQueryMiniBatch received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            ForwardQueryMiniBatchWithState queryWithState = new ForwardQueryMiniBatchWithState(
                     q,
                     filteredActivityList
             );
            sendQueryMiniBatchWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_SCC_TYPE)){
            ForwardQuerySCC q = message.as(Types.FORWARD_QUERY_SCC_TYPE);
            LOGGER.debug("[VertexStorageFn {}] ForwardQuerySCC received", context.self().id());
            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            ForwardQuerySCCWithState queryWithState = new ForwardQuerySCCWithState(
                    q,
                    filteredActivityList
            );
            sendQueryStronglyConnectedComponentWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.FORWARD_QUERY_CC_TYPE)){
            ForwardQueryCC q = message.as(Types.FORWARD_QUERY_CC_TYPE);
            LOGGER.debug("[VertexStorageFn {}] ForwardQueryCC received", context.self().id());
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
                return vertexActivitiesRes1;
            case 2:
                PriorityQueue<VertexActivity> vertexActivitiesPQ = context.storage().get(VERTEX_ACTIVITIES_PQ).orElse(
                        new PriorityQueue<>());
                List<VertexActivity> vertexActivitiesRes2 = new ArrayList<>();

                int len = vertexActivitiesPQ.size();
                for(int i=0; i<len; i++){
                    VertexActivity curActivity = vertexActivitiesPQ.poll();
                    if(Integer.parseInt(curActivity.getTimestamp()) > endT){
                        break;
                    }
                    if(Integer.parseInt(curActivity.getTimestamp()) >= startT) {
                        vertexActivitiesRes2.add(curActivity);
                    }
                }
                return vertexActivitiesRes2;
            case 3:
                int batchIndexStart = startT / eventTimeInterval;
                int batchIndexEnd = endT / eventTimeInterval;
                TreeMap<String, List<VertexActivity>> vertexActivitiesBRBT = context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());

                List<VertexActivity> vertexActivitiesRes3 = new ArrayList<>();
                // just select out the buckets that the activities in the given time region belong to
                for(int i=batchIndexStart; i<=batchIndexEnd; i++){
                    if(vertexActivitiesBRBT.containsKey(String.valueOf(i))){
                        vertexActivitiesRes3.addAll(vertexActivitiesBRBT.get(String.valueOf(i)));
                    }
                }
                // filter out the other activities that do not belong to the time region
                vertexActivitiesRes3.removeIf(v -> Integer.parseInt(v.getTimestamp()) < startT || Integer.parseInt(v.getTimestamp()) > endT);
                return vertexActivitiesRes3;
            default:
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
        }
    }

    @Deprecated
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
                PriorityQueue<VertexActivity> vertexActivitiesPQ = context.storage().get(VERTEX_ACTIVITIES_PQ).orElse(
                        new PriorityQueue<>());
                // append into pq
                vertexActivitiesPQ.add(new VertexActivity("add", temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
                context.storage().set(VERTEX_ACTIVITIES_PQ, vertexActivitiesPQ);
                break;
            case 3:
                TreeMap<String, List<VertexActivity>> vertexActivitiesBRBT = context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());
                int t = Integer.parseInt(temporalEdge.getTimestamp());
                String batchIndex = String.valueOf(t / eventTimeInterval);
                List<VertexActivity> batchActivity = vertexActivitiesBRBT.getOrDefault(batchIndex, new ArrayList<>());
                batchActivity.add(
                        new VertexActivity("add",
                                temporalEdge.getSrcId(), temporalEdge.getDstId(), temporalEdge.getTimestamp()));
                vertexActivitiesBRBT.put(batchIndex, batchActivity);
                context.storage().set(VERTEX_ACTIVITIES_BRBT, vertexActivitiesBRBT);
                break;
            default:
                LOGGER.error("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
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

    @Deprecated
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
