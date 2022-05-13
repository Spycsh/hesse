package io.github.spycsh.hesse;

import io.github.spycsh.hesse.applications.*;
import io.github.spycsh.hesse.benchmarks.BenchmarkFilterTimeFn;
import io.github.spycsh.hesse.benchmarks.BenchmarkStorageTimeFn;
import io.github.spycsh.hesse.coordination.CoordinatorFn;
import io.github.spycsh.hesse.query.TemporalQueryHandlerFn;
//import io.github.spycsh.hesse.storage.ControllerFn;
//import io.github.spycsh.hesse.storage.PartitionManagerFn;
import io.github.spycsh.hesse.storage.VertexStorageFn;
import io.github.spycsh.hesse.undertow.UndertowHttpHandler;
import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

public class hesseAppServer {

    public static void main(String[] args) {
        final StatefulFunctions functions = new StatefulFunctions();

        // register the storage functions
        // options 1: partition by partitionId
        // use controller -> partitionManager -> applications
        // functions.withStatefulFunction(ControllerFn.SPEC);
        // functions.withStatefulFunction(PartitionManagerFn.SPEC);
        // options 2: partition by vertexId
        // use vertexStorage -> applications
        functions.withStatefulFunction(VertexStorageFn.SPEC);

        // register the coordination functions
        functions.withStatefulFunction(CoordinatorFn.SPEC);

        // register the query functions
        functions.withStatefulFunction(TemporalQueryHandlerFn.SPEC);
        functions.withStatefulFunction(MiniBatchFn.SPEC);
        functions.withStatefulFunction(ConnectedComponentsFn.SPEC);
        functions.withStatefulFunction(StronglyConnectedComponentsFn.SPEC);
        functions.withStatefulFunction(SingleSourceShortestPathFn.SPEC);
        functions.withStatefulFunction(PageRankFn.SPEC);

        // register the benchmark functions
        functions.withStatefulFunction(BenchmarkStorageTimeFn.SPEC);
        functions.withStatefulFunction(BenchmarkFilterTimeFn.SPEC);

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();

        final Undertow httpServer =
                Undertow.builder()
                    .addHttpListener(1108, "0.0.0.0")
                    .setHandler(new UndertowHttpHandler(requestReplyHandler))
                    .build();
        httpServer.start();
    }

}
