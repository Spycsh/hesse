package io.github.spycsh.hesse.storage;

import io.github.spycsh.hesse.types.*;
import io.github.spycsh.hesse.types.cc.ForwardQueryCC;
import io.github.spycsh.hesse.types.cc.ForwardQueryCCWithState;
import io.github.spycsh.hesse.types.cc.QueryCC;
import io.github.spycsh.hesse.types.cc.QueryCCWithState;
import io.github.spycsh.hesse.types.ingress.TemporalEdge;
import io.github.spycsh.hesse.types.gnnsampling.ForwardQueryGNNSampling;
import io.github.spycsh.hesse.types.gnnsampling.ForwardQueryGNNSamplingWithState;
import io.github.spycsh.hesse.types.gnnsampling.QueryGNNSampling;
import io.github.spycsh.hesse.types.gnnsampling.QueryGNNSamplingState;
import io.github.spycsh.hesse.types.pagerank.PageRankTask;
import io.github.spycsh.hesse.types.pagerank.PageRankTaskWithState;
import io.github.spycsh.hesse.types.scc.ForwardQuerySCC;
import io.github.spycsh.hesse.types.scc.ForwardQuerySCCWithState;
import io.github.spycsh.hesse.types.scc.QuerySCC;
import io.github.spycsh.hesse.types.scc.QuerySCCWithState;
import io.github.spycsh.hesse.types.sssp.QuerySSSP;
import io.github.spycsh.hesse.types.sssp.QuerySSSPWithState;
import io.github.spycsh.hesse.types.sssp.QueryState;
import io.github.spycsh.hesse.types.sssp.QueryStateRequest;
import io.github.spycsh.hesse.util.CustomizedComparator;
import io.github.spycsh.hesse.util.PropertyFileReader;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * partition by vertexId storage function
 * each vertex storage function instance will be distributed
 * to different Flink StateFun workers by using hash partitioning
 * on its address and the partitioning is transparent to users
 */
public class VertexStorageFn implements StatefulFunction {

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "vertex-storage");

    static int storageParadigm = 2;    // default paradigm

    static boolean storeIngoingEdges = false;

    /**
     * A pre-config event time interval that used to separate
     * the whole sequence of temporal edges into batches
     * e.g. there are three records with timestamp 0, 5, 7
     * and the event time interval is 5
     * then it will have two batches [0, 5), [5, 10)
     * 0 will be in the first batch while 5 and 7 will be in the second one
     *
     * This is specifically used for BRBT storage paradigm 3 and 4
     */
    private static int eventTimeInterval = 5;
    private static int bucketSize = 10;
    private static int T0 = 0;
    private static int Tt = 2000000000;
    private static long IndexTime = 0L;

    static Properties prop;

    static {
        try {
            prop = PropertyFileReader.readPropertyFile();
        } catch (Exception e) {
            e.printStackTrace();
        }

        VertexStorageFn.storageParadigm = Integer.parseInt(prop.getProperty("STORAGE_PARADIGM"));
        VertexStorageFn.eventTimeInterval = Integer.parseInt(prop.getProperty("EVENT_TIME_INTERVAL"));
        VertexStorageFn.T0 = Integer.parseInt(prop.getProperty("T0"));
        VertexStorageFn.Tt = Integer.parseInt(prop.getProperty("Tt"));
        // BUCKET_NUMBER = ceil((Tt – T0)/ TIME_INTERVAL)
        VertexStorageFn.bucketSize = (int) Math.ceil(((double) Tt - (double) T0) / eventTimeInterval);
        VertexStorageFn.storeIngoingEdges = Boolean.parseBoolean(prop.getProperty("STORE_INGOING_EDGES"));
    }

    // valueSpecs of outer vertex activities
    private static final ValueSpec<List<VertexActivity>> VERTEX_ACTIVITIES_LIST = ValueSpec.named("vertexActivitiesList").withCustomType(Types.VERTEX_ACTIVITIES_LIST_TYPE);
    private static final ValueSpec<PriorityQueue<VertexActivity>> VERTEX_ACTIVITIES_PQ = ValueSpec.named("vertexActivitiesPQ").withCustomType(Types.VERTEX_ACTIVITIES_PQ_TYPE);
    private static final ValueSpec<TreeMap<Integer, List<VertexActivity>>> VERTEX_ACTIVITIES_BRBT = ValueSpec.named("vertexActivitiesBRBT").withCustomType(Types.VERTEX_ACTIVITIES_BRBT_TYPE);
    // use one ValueSpec for each bucket
    static TreeMap<String, ValueSpec<List<VertexActivity>>> bucketMap = new TreeMap<String, ValueSpec<List<VertexActivity>>>(new CustomizedComparator());
    static {
        if(VertexStorageFn.storageParadigm == 4){
            for(int i=0; i<VertexStorageFn.bucketSize; i++){
                bucketMap.put(String.valueOf(i), ValueSpec.named("bucket"+i).withCustomType(Types.BUCKET_TYPE));
            }
        }
    }

    // start to build the StatefulFunctionSpec
    static StatefulFunctionSpec.Builder builder = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(VertexStorageFn::new)
            .withValueSpecs(VERTEX_ACTIVITIES_LIST, VERTEX_ACTIVITIES_PQ, VERTEX_ACTIVITIES_BRBT);
    static {
        // if using storage paradigm 4, create the index
        if(VertexStorageFn.storageParadigm == 4){
            VertexStorageFn.IndexTime = System.nanoTime();
            for (Map.Entry<String, ValueSpec<List<VertexActivity>>> e : bucketMap.entrySet()) {
                builder.withValueSpec(e.getValue());
            }

            VertexStorageFn.IndexTime = System.nanoTime() - VertexStorageFn.IndexTime;

        }

    }
    public static final StatefulFunctionSpec SPEC = builder.build();

    private static final TypeName KAFKA_INDEXING_EGRESS = TypeName.typeNameOf("hesse.io", "indexing-time");

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(VertexStorageFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        // read streaming temporal edges and convert them to adjacent list form
        if(message.is(Types.TEMPORAL_EDGE_TYPE)) {
            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);

            if(VertexStorageFn.storageParadigm == 4 && VertexStorageFn.IndexTime != 0L){
                // egress indexing time
                // static field has no atomic control, so this can produce several same index time
                // but it is just a benchmark metric for one storage paradigm so it does not matter
                egressIndexTime(context, VertexStorageFn.IndexTime);
            }

            // store the new activity of into specified batch one single vertex in its context
            // here the default activity type is add
            // should use this to recover to specified state and serve query
            long storageStartTime = System.nanoTime();
            storeActivity(context, new VertexActivity("add", temporalEdge, false));
            if(VertexStorageFn.storeIngoingEdges) {
                storeIngoingActivity(context, new VertexActivity("add", temporalEdge, true));
            }
            long storageEndTime = System.nanoTime();
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.benchmarks", "benchmark-storage-time"), "1")
                    .withValue(storageEndTime - storageStartTime)
                    .build());

            // send all the nodes ids to Coordinator 0
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.coordination", "coordinator"), "0")
                    .withValue(temporalEdge.getSrcId() + " " + temporalEdge.getDstId())
                    .build());
        }


        if(message.is(Types.VERTEX_ACTIVITY_TYPE)){
            VertexActivity activity = message.as(Types.VERTEX_ACTIVITY_TYPE);
            long storageStartTime = System.nanoTime();
            storeActivity(context, activity);
            long storageEndTime = System.nanoTime();
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.benchmarks", "benchmark-storage-time"), "1")
                    .withValue(storageEndTime - storageStartTime)
                    .build());
        }

        // handle the queries of mini batch
        if(message.is(Types.QUERY_GNN_SAMPLING_TYPE)){
            QueryGNNSampling q = message.as(Types.QUERY_GNN_SAMPLING_TYPE);
            LOGGER.info("[VertexStorageFn {}] QueryGNNSampling received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            // only send the needed log
            // namely from the beginning to the batch which time T is in
            // remove all the log that is later than the time region that the batch index corresponding to
            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());

            QueryGNNSamplingState queryWithState = new QueryGNNSamplingState(
                    q,
                    filteredActivityList
            );
            sendQueryGNNSamplingWithStateToApp(context, queryWithState);
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

        if(message.is(Types.QUERY_SSSP_TYPE)) {
            QuerySSSP q = message.as(Types.QUERY_SSSP_TYPE);
            LOGGER.info("[VertexStorageFn {}] QuerySingleSourceShortestPath received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            QuerySSSPWithState queryWithState = new QuerySSSPWithState(
                    q,
                    filteredActivityList
            );
            sendQuerySingleSourceShortestPathWithStateToApp(context, queryWithState);
        }

        if(message.is(Types.PAGERANK_TASK_TYPE)){
            PageRankTask q = message.as(Types.PAGERANK_TASK_TYPE);
            LOGGER.info("[VertexStorageFn {}] PageRankTask received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            PageRankTaskWithState taskWithState = new PageRankTaskWithState(
                    q,
                    filteredActivityList
            );
            sendPageRankTaskWithStateToApp(context, taskWithState);
        }

        if(message.is(Types.FORWARD_QUERY_GNN_SAMPLING_TYPE)){
            ForwardQueryGNNSampling q = message.as(Types.FORWARD_QUERY_GNN_SAMPLING_TYPE);
            LOGGER.info("[VertexStorageFn {}] ForwardQueryGNNSampling received with startT:{}, endT:{}", context.self().id(), q.getStartT(), q.getEndT());

            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());
            ForwardQueryGNNSamplingWithState queryWithState = new ForwardQueryGNNSamplingWithState(
                     q,
                     filteredActivityList
             );
            sendQueryGNNSamplingWithStateToApp(context, queryWithState);
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

        if(message.is(Types.QUERY_STATE_REQUEST_TYPE)){
            QueryStateRequest q = message.as(Types.QUERY_STATE_REQUEST_TYPE);
            LOGGER.debug("[VertexStorageFn {}] QueryWeightedStateRequest received", context.self().id());
            List<VertexActivity> filteredActivityList = filterActivityListByTimeRegion(context, q.getStartT(), q.getEndT());

            QueryState state = new QueryState(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getStartT(), q.getEndT(), filteredActivityList);
            sendQueryStateToApp(context, state);
        }

        return context.done();
    }

    // forward the ingoing vertex activity to destination node
    // to store ingoing activity in the destination node's context
    private void storeIngoingActivity(Context context, VertexActivity activity) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), activity.getDstId())
                .withCustomType(
                        Types.VERTEX_ACTIVITY_TYPE,
                        activity)
                .build());
    }

    private List<VertexActivity> filterActivityListByTimeRegion(Context context, int startT, int endT) {
        long filterStartTime = System.nanoTime();
        List<VertexActivity> vertexActivitiesRes = new LinkedList<>();
        switch (storageParadigm){
            case 1: {
                List<VertexActivity> vertexActivitiesList = context.storage().get(VERTEX_ACTIVITIES_LIST).orElse(new LinkedList<>());
                for (VertexActivity curActivity : vertexActivitiesList) {
                    if (Integer.parseInt(curActivity.getTimestamp()) <= endT && Integer.parseInt(curActivity.getTimestamp()) >= startT) {
                        vertexActivitiesRes.add(curActivity);
                    }
                }
                break;
            }
            case 2: {
                PriorityQueue<VertexActivity> vertexActivitiesPQ = context.storage().get(VERTEX_ACTIVITIES_PQ).orElse(
                        new PriorityQueue<>());

                PriorityQueue<VertexActivity> tempPQ = new PriorityQueue<>(vertexActivitiesPQ);
                int len = tempPQ.size();
                for (int i = 0; i < len; i++) {
                    VertexActivity curActivity = tempPQ.poll();
                    if (Integer.parseInt(curActivity.getTimestamp()) > endT) {
                        break;
                    }
                    if (Integer.parseInt(curActivity.getTimestamp()) >= startT) {
                        vertexActivitiesRes.add(curActivity);
                    }
                }
                break;
            }
            case 3: {
                int batchIndexStart = startT / eventTimeInterval;
                int batchIndexEnd = endT / eventTimeInterval;
                TreeMap<Integer, List<VertexActivity>> vertexActivitiesBRBT = context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());

                // just select out the buckets that the activities in the given time region belong to
                for (int i = batchIndexStart; i <= batchIndexEnd; i++) {
                    if (vertexActivitiesBRBT.containsKey(i)) {
                        vertexActivitiesRes.addAll(vertexActivitiesBRBT.get(i));
                    }
                }
                // filter out the other activities that do not belong to the time region
                vertexActivitiesRes.removeIf(v -> Integer.parseInt(v.getTimestamp()) < startT || Integer.parseInt(v.getTimestamp()) > endT);
                break;
            }
            case 4: {
                int batchIndexStart = startT / eventTimeInterval;
                int batchIndexEnd = endT / eventTimeInterval;

                for (int i = batchIndexStart; i <= batchIndexEnd; i++) {
                    if (bucketMap.containsKey(String.valueOf(i))) {
                        List<VertexActivity> list = context.storage().get(bucketMap.get(String.valueOf(i))).orElse(new LinkedList<>());
                        vertexActivitiesRes.addAll(list);
                    }
                }
                vertexActivitiesRes.removeIf(v -> Integer.parseInt(v.getTimestamp()) < startT || Integer.parseInt(v.getTimestamp()) > endT);
                break;
            }
            default: {
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
            }
        }

        long filterEndTime = System.nanoTime();
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.benchmarks", "benchmark-filter-time"), "1")
                .withValue(filterEndTime - filterStartTime)
                .build());
        return vertexActivitiesRes;
    }


    private void storeActivity(Context context, VertexActivity activity) {
        switch (storageParadigm){
            case 1: {
                List<VertexActivity> vertexActivitiesList = context.storage().get(VERTEX_ACTIVITIES_LIST).orElse(new LinkedList<>());
                // append into list
                vertexActivitiesList.add(activity);
                context.storage().set(VERTEX_ACTIVITIES_LIST, vertexActivitiesList);
                break;
            }
            case 2: {
                PriorityQueue<VertexActivity> vertexActivitiesPQ = context.storage().get(VERTEX_ACTIVITIES_PQ).orElse(
                        new PriorityQueue<>());
                // append into pq
                vertexActivitiesPQ.add(activity);
                context.storage().set(VERTEX_ACTIVITIES_PQ, vertexActivitiesPQ);
                break;
            }
            case 3: {
                TreeMap<Integer, List<VertexActivity>> vertexActivitiesBRBT =
                        context.storage().get(VERTEX_ACTIVITIES_BRBT).orElse(new TreeMap<>());
                int t = Integer.parseInt(activity.getTimestamp());
                int batchIndex = t / eventTimeInterval;
                List<VertexActivity> batchActivity = vertexActivitiesBRBT.getOrDefault(batchIndex, new LinkedList<>());
                batchActivity.add(activity);
                vertexActivitiesBRBT.put(batchIndex, batchActivity);

                context.storage().set(VERTEX_ACTIVITIES_BRBT, vertexActivitiesBRBT);
                break;
            }
            case 4: {
                int t = Integer.parseInt(activity.getTimestamp());
                String bucketIndex = calculateBucketIndex(T0, eventTimeInterval, t);
                // System.out.println(bucketIndex + ":" + bucketMap.size() + ":" + t);
                List<VertexActivity> vertexActivities = context.storage().get(bucketMap.get(bucketIndex)).orElse(new LinkedList<>());
                vertexActivities.add(activity);
                context.storage().set(bucketMap.get(bucketIndex), vertexActivities);
                break;
            }
            default: {
                LOGGER.error("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
                throw new IllegalArgumentException("[VertexStorageFn] Wrong storage paradigm index, please check property file!");
            }
        }
    }

    // BUCKET_INDEX = floor((t – T0)/TIME_INTERVAL)
    private String calculateBucketIndex(int t0, int eventTimeInterval, int t) {
        return String.valueOf((int) Math.floor(((double) t - (double) t0) / eventTimeInterval));
    }

    private void sendQueryGNNSamplingWithStateToApp(Context context, QueryGNNSamplingState queryWithState) {
        context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.applications", "gnn-sampling"), context.self().id())
                        .withCustomType(
                                Types.QUERY_GNN_SAMPLING_WITH_STATE_TYPE,
                                queryWithState)
                        .build());
    }

    private void sendQueryGNNSamplingWithStateToApp(Context context, ForwardQueryGNNSamplingWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "gnn-sampling"), context.self().id())
                .withCustomType(
                        Types.FORWARD_QUERY_GNN_SAMPLING_WITH_STATE_TYPE,
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

    private void sendQuerySingleSourceShortestPathWithStateToApp(Context context, QuerySSSPWithState queryWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "single-source-shortest-path"), context.self().id())
                .withCustomType(
                        Types.QUERY_SSSP_WITH_STATE_TYPE,
                        queryWithState)
                .build());
    }

    private void sendQueryStateToApp(Context context, QueryState state) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "single-source-shortest-path"), context.self().id())
                .withCustomType(
                        Types.QUERY_STATE_TYPE,
                        state)
                .build());
    }


    private void sendPageRankTaskWithStateToApp(Context context, PageRankTaskWithState taskWithState) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.applications", "pagerank"), context.self().id())
                .withCustomType(
                        Types.PAGERANK_TASK_WITH_STATE_TYPE,
                        taskWithState)
                .build());
    }

    private void egressIndexTime(Context context, long indexTime) {
        context.send(KafkaEgressMessage.forEgress(KAFKA_INDEXING_EGRESS)
                .withTopic("indexing-time")
                .withUtf8Key("indexing")
                .withUtf8Value(String.valueOf(indexTime))
                .build());
    }
}
