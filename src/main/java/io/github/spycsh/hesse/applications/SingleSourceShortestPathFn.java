package io.github.spycsh.hesse.applications;

import io.github.spycsh.hesse.types.Types;
import io.github.spycsh.hesse.types.VertexActivity;
import io.github.spycsh.hesse.types.egress.QueryResult;
import io.github.spycsh.hesse.types.sssp.*;
import io.github.spycsh.hesse.util.Utils;
import org.apache.flink.statefun.sdk.java.*;
import org.apache.flink.statefun.sdk.java.message.Message;
import org.apache.flink.statefun.sdk.java.message.MessageBuilder;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class SingleSourceShortestPathFn implements StatefulFunction {

    static final TypeName TYPE_NAME = TypeName.typeNameOf("hesse.applications", "single-source-shortest-path");
    private static final ValueSpec<ArrayList<QuerySSSPContext>> QUERY_SSSP_CONTEXT_LIST =
            ValueSpec.named("querySSSPContext").withCustomType(Types.QUERY_SSSP_CONTEXT_LIST_TYPE);
    public static final StatefulFunctionSpec SPEC = StatefulFunctionSpec.builder(TYPE_NAME)
            .withSupplier(SingleSourceShortestPathFn::new)
            .withValueSpecs(QUERY_SSSP_CONTEXT_LIST)
            .build();

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(SingleSourceShortestPathFn.class);

    @Override
    public CompletableFuture<Void> apply(Context context, Message message) throws Throwable {

        if(message.is(Types.QUERY_SSSP_WITH_STATE_TYPE)){
            LOGGER.debug("[SingleSourceShortestPathFn {}] QuerySSSPWithState received", context.self().id());

            QuerySSSPWithState q = message.as(Types.QUERY_SSSP_WITH_STATE_TYPE);
            List<VertexActivity> vertexActivities = q.getVertexActivities();

            // initialize the vertexSet, which is the set that already find a shorted path
            // at first only itself is included
            HashSet<String> vertexSet = new HashSet<String>(){{
                add(context.self().id());
            }};

            // initialize the dist map, which holds the distance information
            // just initialize with weight for direct neighbours
            // the indirect neighbours do not have an entry and will be dynamically added
            Map<String, String> neighbourIdsWithWeight = Utils.recoverWeightedStateByLog(vertexActivities);

            // for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()){
            //     System.out.println(e.getKey() +":"+e.getValue());
            // }

            // System.out.println(neighbourIdsWithWeight.size());

            // if there are no outer edges, directly egress
            if(neighbourIdsWithWeight.size() == 0){
                StringBuilder result = new StringBuilder();
                String str1 = String.format("Result of query %s by user %s: no shortest path from node %s because there are no outgoing edge from it!",
                        q.getQueryId(), q.getUserId(), context.self().id());
                result.append(str1);
                sendResult(context, q.getQueryId(), q.getUserId(), result.toString());
            } else{
                // initialize the map of shortest path information for each vertex
                Map<String, List<String>> paths = new HashMap<>();
                // only add the minimum weight path
                double minimumWeight = Double.MAX_VALUE;
                StringBuilder minimumWeightId = new StringBuilder();

                ArrayList<QuerySSSPContext> querySSSPContexts = context.storage().get(QUERY_SSSP_CONTEXT_LIST).orElse(new ArrayList<>());

                neighbourIdsWithWeight.put(context.self().id(), "0.0");
                querySSSPContexts.add(new QuerySSSPContext(q.getQueryId(), q.getUserId(), q.getStartT(), q.getEndT(), vertexSet, neighbourIdsWithWeight, paths));

                context.storage().set(QUERY_SSSP_CONTEXT_LIST, querySSSPContexts);

                for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()){
                    String id = e.getKey();
                    if(id.equals(context.self().id())){
                        continue;
                    }
                    String weight = e.getValue();
                    if(Double.parseDouble(weight) < minimumWeight){
                        minimumWeight = Double.parseDouble(weight);
                        minimumWeightId = new StringBuilder(id);
                    }

                    paths.put(id, new LinkedList<String>(){{
                        add(context.self().id());
                        add(String.valueOf(id));
                    }});
                }

                String minimumWeightIdStr = minimumWeightId.toString();

                // persist state
                context.storage().set(QUERY_SSSP_CONTEXT_LIST, querySSSPContexts);

                // send to the vertex minimumWeightId the request of neighbours of it
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), minimumWeightIdStr)
                        .withCustomType(
                                Types.QUERY_STATE_REQUEST_TYPE,
                                new QueryStateRequest(q.getQueryId(), q.getUserId(), q.getVertexId(), q.getStartT(), q.getEndT())
                        )
                        .build());
            }

        }

        if(message.is(Types.QUERY_STATE_TYPE)){
            LOGGER.debug("[SingleSourceShortestPathFn {}] QueryState received", context.self().id());
            QueryState q = message.as(Types.QUERY_STATE_TYPE);

            Map<String, String> neighbourIdsWithWeight = Utils.recoverWeightedStateByLog(q.getVertexActivities());

            // send to source id its neighbour info
            context.send(MessageBuilder
                    .forAddress(TypeName.typeNameOf("hesse.applications", "single-source-shortest-path"), q.getVertexId())
                    .withCustomType(
                            Types.QUERY_NEXT_NEIGHBOURS_INFO,
                            new QueryNextNeighboursInfo(q.getQueryId(), q.getUserId(), q.getVertexId(),
                                    context.self().id(), neighbourIdsWithWeight)
                    )
                    .build());
        }

        if(message.is(Types.QUERY_NEXT_NEIGHBOURS_INFO)){
            LOGGER.debug("[SingleSourceShortestPathFn {}] QueryNextNeighbourInfo received", context.self().id());
            QueryNextNeighboursInfo q = message.as(Types.QUERY_NEXT_NEIGHBOURS_INFO);

            Map<String, String> nextNeighbourIdsWithWeight = q.getNeighbourIdsWithWeight();

            ArrayList<QuerySSSPContext> querySSSPContexts = context.storage().get(QUERY_SSSP_CONTEXT_LIST).orElse(new ArrayList<>());
            QuerySSSPContext querySSSPContext = findSSSPContext(q.getQueryId(), q.getUserId(), querySSSPContexts);

            if(querySSSPContext == null){
                LOGGER.error("[SingleSourceShortestPathFn {}] querySSSPContext should not be null!", context.self().id());
                throw new IllegalStateException("querySSSPContext should not be null!\n");
            }
            Map<String, String> neighbourIdsWithWeight = querySSSPContext.getNeighbourIdsWithWeight();
            Map<String, List<String>> paths = querySSSPContext.getPaths();
            HashSet<String> vertexSet = querySSSPContext.getVertexSet();
            // get the distance to the queried neighbour
            String distToNeighbour = neighbourIdsWithWeight.get(q.getSourceId());

            // update the neighbourIdsWithWeight with the nextNeighbourIdsWithWeight
            for(Map.Entry<String, String> e:nextNeighbourIdsWithWeight.entrySet()){
                String id1 = e.getKey();
                double newDistToNextNeighbour = Double.parseDouble(distToNeighbour) + Double.parseDouble(e.getValue());
                if(neighbourIdsWithWeight.containsKey(id1)){
                    if(newDistToNextNeighbour <= Double.parseDouble(neighbourIdsWithWeight.get(id1))){
                        // update the entry in the dist map and paths map only if it has shorter distance
                        neighbourIdsWithWeight.put(id1, String.valueOf(newDistToNextNeighbour));

                        LinkedList<String> path = new LinkedList<>(paths.get(q.getSourceId()));
                        path.add(id1);
                        paths.put(id1, path);
                    }
                } else {
                    // directly add an entry in the dist map and paths map
                    neighbourIdsWithWeight.put(id1, String.valueOf(newDistToNextNeighbour));

                    LinkedList<String> path = new LinkedList<>(paths.get(q.getSourceId()));
                    path.add(id1);
                    paths.put(id1, path);
                }
            }

            // only add the minimum weight path
            double minimumWeight = Double.MAX_VALUE;
            StringBuilder minimumWeightId = new StringBuilder();

            // if all neighbour ids equals to vertexSet, it means all the approachable nodes find shortest path, egress
            if(neighbourIdsWithWeight.keySet().equals(vertexSet)){
                StringBuilder result = new StringBuilder();
                String str1 = String.format("Result of query %s by user %s: shortest path(s) from node %s to others:\n",
                        q.getQueryId(), q.getUserId(), context.self().id());
                result.append(str1);
                for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()){
                    String id = e.getKey();
                    String weight = e.getValue();
                    String path;
                    if(!id.equals(q.getVertexId())) {
                        path = paths.get(id).toString();
                        result.append("\tid: ").append(id).append(" distance: ").append(weight).append(" path: ").append(path).append("\n");
                    }
                }
                querySSSPContexts.remove(querySSSPContext);
                System.out.println("yyyy");
                sendResult(context, q.getQueryId(), q.getUserId(), result.toString());
            } else{
                // find the shortest path vertex among those not in the vertexSet and send to it the state request
                for(Map.Entry<String, String> e:neighbourIdsWithWeight.entrySet()){
                    String id = e.getKey();
                    if(vertexSet.contains(id)){
                        continue;
                    }
                    String weight = e.getValue();
                    if(Double.parseDouble(weight) < minimumWeight){
                        minimumWeight = Double.parseDouble(weight);
                        minimumWeightId = new StringBuilder(id);
                    }
                }

                String minimumWeightIdStr = minimumWeightId.toString();

                // update the vertexSet
                vertexSet.add(minimumWeightIdStr);

                context.storage().set(QUERY_SSSP_CONTEXT_LIST, querySSSPContexts);

                // send to the vertex minimumWeightId the request of neighbours of it
                context.send(MessageBuilder
                        .forAddress(TypeName.typeNameOf("hesse.storage", "vertex-storage"), minimumWeightIdStr)
                        .withCustomType(
                                Types.QUERY_STATE_REQUEST_TYPE,
                                new QueryStateRequest(q.getQueryId(), q.getUserId(), q.getVertexId(), querySSSPContext.getStartT(), querySSSPContext.getEndT())
                        )
                        .build());
            }
        }

        return context.done();
    }

    private QuerySSSPContext findSSSPContext(String queryId, String userId, ArrayList<QuerySSSPContext> list) {
        for(QuerySSSPContext e: list) {
            if (e.getQueryId().equals(queryId) && e.getUserId().equals(userId)) {
                return e;
            }
        }
        return null;
    }

    private void sendResult(Context context, String queryId, String userId, String resultStr) {
        context.send(MessageBuilder
                .forAddress(TypeName.typeNameOf("hesse.query", "temporal-query-handler"), queryId)
                .withCustomType(
                        Types.QUERY_RESULT_TYPE,
                        new QueryResult(queryId, userId, resultStr))
                .build());
    }
}
