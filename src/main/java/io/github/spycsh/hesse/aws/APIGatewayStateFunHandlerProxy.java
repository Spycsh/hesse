package io.github.spycsh.hesse.aws;

import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2ProxyResponseEvent;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.flink.statefun.sdk.java.handler.RequestReplyHandler;
import org.apache.flink.statefun.sdk.java.slice.Slice;
import org.apache.flink.statefun.sdk.java.slice.Slices;

final class APIGatewayStateFunHandlerProxy {

  private static final Map<String, String> RESPONSE_HEADER = new HashMap<>(1);

  static {
    RESPONSE_HEADER.put("Content-Type", "application/octet-stream");
  }

  private final RequestReplyHandler stateFunHandler;

  APIGatewayStateFunHandlerProxy(RequestReplyHandler stateFunHandler) {
    this.stateFunHandler = Objects.requireNonNull(stateFunHandler);
  }

  public APIGatewayV2ProxyResponseEvent handle(APIGatewayV2ProxyRequestEvent request) {
    // Binary blobs (the invocation request) are always base64 encoded by the API Gateway
    final byte[] decoded = Base64.getDecoder().decode(request.getBody());

    final Slice responseSlice = stateFunHandler.handle(Slices.wrap(decoded)).join();
    return createResponse(responseSlice);
  }

  private static APIGatewayV2ProxyResponseEvent createResponse(Slice responseSlice) {
    final APIGatewayV2ProxyResponseEvent response = new APIGatewayV2ProxyResponseEvent();
    response.setHeaders(RESPONSE_HEADER);
    response.setIsBase64Encoded(true);
    response.setStatusCode(200);
    response.setBody(Base64.getEncoder().encodeToString(responseSlice.toByteArray()));
    return response;
  }
}
