package io.github.spycsh.hesse.undertow;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;


public class UndertowHttpHandler implements HttpHandler{
    private final RequestReplyHandler handler;

    public UndertowHttpHandler(RequestReplyHandler handler) {
        this.handler = Objects.requireNonNull(handler);
    }

    @Override
    public void handleRequest(HttpServerExchange httpServerExchange) throws Exception {
        httpServerExchange.getRequestReceiver().receiveFullBytes(this::onRequestBody);
    }

    private void onRequestBody(HttpServerExchange exchange, byte[] requestBytes) {
        exchange.dispatch();
        CompletableFuture<Slice> future = handler.handle(Slices.wrap(requestBytes));
        future.whenComplete((response, exception) -> onComplete(exchange, response, exception));
    }

    private void onComplete(HttpServerExchange exchange, Slice responseBytes, Throwable ex) {
        if (ex != null){
            ex.printStackTrace(System.out);
            exchange.getResponseHeaders().put(Headers.STATUS, 500);
            exchange.endExchange();
            return;
        }
        exchange.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
        exchange.getResponseSender().send(responseBytes.asReadOnlyByteBuffer());
    }


}
