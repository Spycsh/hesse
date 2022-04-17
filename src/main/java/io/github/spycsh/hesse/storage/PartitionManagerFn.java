//package io.github.spycsh.hesse.storage;
//
//import io.github.spycsh.hesse.types.*;
//import io.github.spycsh.hesse.types.ingress.TemporalEdge;
//import io.github.spycsh.hesse.types.ingress.TemporalWeightedEdge;
//import io.github.spycsh.hesse.util.PropertyFileReader;
//import org.apache.flink.statefun.sdk.java.*;
//import org.apache.flink.statefun.sdk.java.io.KafkaEgressMessage;
//import org.apache.flink.statefun.sdk.java.message.Message;
//import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
//
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//
//public class PartitionManagerFn implements StatefulFunction {
//
//    // every partition manager must hold a partition id
//    private static final ValueSpec<String> PARTITION_ID = ValueSpec.named("partitionId").withUtf8StringType();
//    // temporal edge
//    private static final ValueSpec<TemporalEdge> TEMPORAL_EDGE = ValueSpec.named("temporalEdge").withCustomType(Types.TEMPORAL_EDGE_TYPE);
//    // a set of temporal edges
//    private static final ValueSpec<HashSet<TemporalEdge>> TEMPORAL_EDGES =
//            ValueSpec.named("temporalEdges").withCustomType(Types.TEMPORAL_EDGES_TYPE);
//
//    private static final ValueSpec<TemporalWeightedEdge> TEMPORAL_WEIGHTED_EDGE = ValueSpec.named("temporalWeightedEdge").withCustomType(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);
//    private static final ValueSpec<HashSet<TemporalWeightedEdge>> TEMPORAL_WEIGHTED_EDGES =
//            ValueSpec.named("temporalWeightedEdges").withCustomType(Types.TEMPORAL_EDGES_WEIGHTED_TYPE);
//
////    private static final ValueSpec<TreeMap<Integer, ArrayList<VertexActivity>>> VERTEX_ACTIVITIES = ValueSpec.named("vertexActivities").withCustomType(Types.VERTEX_ACTIVITIES);
//    private static final ValueSpec<HashMap<String, TreeMap<Integer, ArrayList<VertexActivity>>>> PARTITION_ACTIVITIES =
//        ValueSpec.named("partitionActivities").withCustomType(Types.PARTITION_ACTIVITIES);
//
//    List<String> unweightedAppNames = new ArrayList<>();
//    List<String> weightedAppNames = new ArrayList<>();
//
//    public void setUnWeightedAppNames(String appNames) {
//        this.unweightedAppNames.addAll(Arrays.asList(appNames.split(",")));
//    }
//
//    public void setWeightedAppNames(String appNames) {
//        this.weightedAppNames.addAll(Arrays.asList(appNames.split(",")));
//    }
//
//    Properties prop;
//
//    private static final int EVENT_TIME_INTERVAL = 5;
//
//    {
//        try {
//            prop = PropertyFileReader.readPropertyFile();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        setUnWeightedAppNames(prop.getProperty("UNWEIGHTED_APP_NAMES"));
//        setWeightedAppNames(prop.getProperty("WEIGHTED_APP_NAMES"));
//    }
//
//    public static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.storage", "partitionManager");
//    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec
//            .builder(TYPE_NAME)
//            .withSupplier(PartitionManagerFn::new)
//            .withValueSpecs(PARTITION_ID, TEMPORAL_EDGE, TEMPORAL_EDGES, TEMPORAL_WEIGHTED_EDGE, TEMPORAL_WEIGHTED_EDGES, PARTITION_ACTIVITIES)
//            .build();
//
//    static final TypeName KAFKA_EGRESS = TypeName.typeNameOf("hesse.io", "partition-edges");
//
//    @Override
//    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {
//        if(message.is(Types.PARTITION_CONFIG_TYPE)){
//            PartitionConfig config = message.as(Types.PARTITION_CONFIG_TYPE);
//            context.storage().set(PARTITION_ID, config.getPartitionId());
//        }
//
//        // TODO OPERATION ADD, DELETE, EDIT
//        // if a partition receives the temporal edge route to it
//        // it will store in the internal buffer
//        if(message.is(Types.TEMPORAL_EDGE_TYPE)){
//            // get current set of temporal edges
////            HashSet<TemporalEdge> temporalEdges = context.storage().get(TEMPORAL_EDGES).orElse(new HashSet<>());
//            // get the temporal edge that needed to be added
//            TemporalEdge temporalEdge = message.as(Types.TEMPORAL_EDGE_TYPE);
////            temporalEdges.add(temporalEdge);
////            context.storage().set(TEMPORAL_EDGES, temporalEdges);
//
//            // TODO persistence, event time batch checkpointing
//            storeActivity(context, temporalEdge);
//
//            outputTemporalEdgeChange(context, temporalEdge);
//            sendNeighbourToApplications(context, temporalEdge);
//        } else if(message.is(Types.TEMPORAL_EDGE_WEIGHTED_TYPE)){
//            // get current set of temporal edges
////            HashSet<TemporalWeightedEdge> temporalWeightedEdges = context.storage().get(TEMPORAL_WEIGHTED_EDGES).orElse(new HashSet<>());
//            // get the temporal edge that needed to be added
//            TemporalWeightedEdge temporalWeightedEdge = message.as(Types.TEMPORAL_EDGE_WEIGHTED_TYPE);
////            temporalWeightedEdges.add(temporalWeightedEdge);
////            context.storage().set(TEMPORAL_WEIGHTED_EDGES, temporalWeightedEdges);
//            // TODO persistence, event time batch checkpointing
//            storeActivity(context, temporalWeightedEdge);
//
//            outputTemporalEdgeChange(context, temporalWeightedEdge);
//            sendNeighbourToApplications(context, temporalWeightedEdge);
//        }
//
//        return context.done();
//    }
//
//    private void outputTemporalEdgeChange(Context context, TemporalEdge edge){
//        String partitionId = context.self().id();
//        context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
//                .withTopic("partition-edges")
//                .withUtf8Key(String.valueOf(partitionId))
//                .withUtf8Value(String.format(
//                        "partition %s: Newly added edge %s -> %s at time %s",
//                        partitionId, edge.getSrcId(), edge.getDstId(), edge.getTimestamp()))
//                .build());
//    }
//
//    private void outputTemporalEdgeChange(Context context, TemporalWeightedEdge edge){
//        String partitionId = context.self().id();
//        context.send(KafkaEgressMessage.forEgress(KAFKA_EGRESS)
//                .withTopic("partition-edges")
//                .withUtf8Key(String.valueOf(partitionId))
//                .withUtf8Value(String.format(
//                        "partition %s: Newly added edge %s -> %s at time %s with weight %s",
//                        partitionId, edge.getSrcId(), edge.getDstId(), edge.getTimestamp(), edge.getWeight()))
//                .build());
//    }
//
//    // send the buffered neighbours to the specified UNWEIGHTED applications
//    private void sendNeighbourToApplications(Context context, TemporalEdge edge) {
//        // send one record buffer at a time
//        HashSet<Integer> bufferedNeighbours = new HashSet<Integer>(){{
//            add(Integer.parseInt(edge.getDstId()));
//        }};
//
//        for(String appName : unweightedAppNames){
//            context.send(MessageBuilder
//                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), edge.getSrcId())
//                    .withCustomType(
//                            Types.BUFFERED_NEIGHBOURS_VALUE,
//                            bufferedNeighbours)
//                    .build());
//        }
//    }
//
//    // send the buffered neighbours to the specified WEIGHTED applications
//    private void sendNeighbourToApplications(Context context, TemporalWeightedEdge edge) {
//        // send one record buffer at a time
//        HashMap<Integer, Double> bufferedWeightedNeighbours = new HashMap<Integer, Double>(){{
//            put(Integer.parseInt(edge.getDstId()), Double.parseDouble(edge.getWeight()));
//        }};
//        for(String appName : weightedAppNames){
//            context.send(MessageBuilder
//                    .forAddress(TypeName.typeNameOf("hesse.applications", appName), edge.getSrcId())
//                    .withCustomType(
//                            Types.BUFFERED_NEIGHBOURS_WEIGHTED_VALUE,
//                            bufferedWeightedNeighbours)
//                    .build());
//        }
//    }
//
//    private void storeActivity(Context context, TemporalEdge edge) {
//
//        HashMap<String, TreeMap<Integer, ArrayList<VertexActivity>>> partitionActivities = context.storage().get(PARTITION_ACTIVITIES).orElse(new HashMap<>());
//        TreeMap<Integer, ArrayList<VertexActivity>> vertexActivities = partitionActivities.getOrDefault(edge.getSrcId(), new TreeMap<>());
//        int batchIndex = Integer.parseInt(edge.getTimestamp()) / EVENT_TIME_INTERVAL;
//        ArrayList<VertexActivity> batchActivity = vertexActivities.getOrDefault(batchIndex, new ArrayList<>());
//        batchActivity.add(
//                new VertexActivity("add",
//                        edge.getSrcId(), edge.getDstId(), edge.getTimestamp()));
//
//        vertexActivities.put(batchIndex, batchActivity);
//        partitionActivities.put(edge.getSrcId(), vertexActivities);
//
//        context.storage().set(PARTITION_ACTIVITIES, partitionActivities);
//    }
//
//    private void storeActivity(Context context, TemporalWeightedEdge edge) {
//        HashMap<String, TreeMap<Integer, ArrayList<VertexActivity>>> partitionActivities = context.storage().get(PARTITION_ACTIVITIES).orElse(new HashMap<>());
//        TreeMap<Integer, ArrayList<VertexActivity>> vertexActivities = partitionActivities.getOrDefault(edge.getSrcId(), new TreeMap<>());
//        int batchIndex = Integer.parseInt(edge.getTimestamp()) / EVENT_TIME_INTERVAL;
//        ArrayList<VertexActivity> batchActivity = vertexActivities.getOrDefault(batchIndex, new ArrayList<>());
//        batchActivity.add(
//                new VertexActivity("add",
//                        edge.getSrcId(), edge.getDstId(), edge.getWeight(), edge.getTimestamp()));
//
//        vertexActivities.put(batchIndex, batchActivity);
//        partitionActivities.put(edge.getSrcId(), vertexActivities);
//
//        context.storage().set(PARTITION_ACTIVITIES, partitionActivities);
//    }
//}
