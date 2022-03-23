package io.github.spycsh.hesse;

import io.github.spycsh.hesse.applications.*;
import io.github.spycsh.hesse.query.TemporalQueryHandlerFn;
import io.github.spycsh.hesse.storage.ControllerFn;
import io.github.spycsh.hesse.storage.PartitionManagerFn;
import io.github.spycsh.hesse.storage.VertexStorageFn;
import io.github.spycsh.hesse.undertow.UndertowHttpHandler;
import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

public class hesseAppServer {

    public static void main(String[] args) {
        final StatefulFunctions functions = new StatefulFunctions();
        // options 1: partition by partitionId
        // use controller -> partitionManager -> applications
        functions.withStatefulFunction(ControllerFn.SPEC);
        functions.withStatefulFunction(PartitionManagerFn.SPEC);
        // options 2: partition by vertexId
        // use vertexStorage -> applications
        functions.withStatefulFunction(VertexStorageFn.SPEC);
        functions.withStatefulFunction(StreamingConnectedComponentsFn.SPEC);
        functions.withStatefulFunction(StreamingSingleSourceShortestPathFn.SPEC);

        // register the query functions
        functions.withStatefulFunction(TemporalQueryHandlerFn.SPEC);
        functions.withStatefulFunction(MiniBatchFn.SPEC);
        functions.withStatefulFunction(StronglyConnectedComponentsFn.SPEC);

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();

        final Undertow httpServer =
                Undertow.builder()
                    .addHttpListener(1108, "0.0.0.0")
                    .setHandler(new UndertowHttpHandler(requestReplyHandler))
                    .build();
        httpServer.start();
    }

}
