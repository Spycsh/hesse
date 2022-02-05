package io.github.spycsh.hesse;

import io.github.spycsh.hesse.undertow.UndertowHttpHandler;
import io.undertow.Undertow;
import org.apache.flink.statefun.sdk.java.StatefulFunctions;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;

public class hesseAppServer {

    public static void main(String[] args) {
        final StatefulFunctions functions = new StatefulFunctions();
        functions.withStatefulFunction(PartitionManagerFn.SPEC);
        functions.withStatefulFunction(ControllerFn.SPEC);

        final RequestReplyHandler requestReplyHandler = functions.requestReplyHandler();
        final Undertow httpServer =
                Undertow.builder()
                    .addHttpListener(1108, "0.0.0.0")
                    .setHandler(new UndertowHttpHandler(requestReplyHandler))
                    .build();
        httpServer.start();
    }

}
